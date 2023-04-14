import base64
import io
from pprint import pprint
from ulid import ULID

import msgpack
import typing
import unittest
from collections import namedtuple
from struct import unpack
from typing import Optional

import zstd
from xxhash import xxh64

# from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# TODO: add support for encrypted values
# TODO: comments on all functions


# Define namedtuple for TLV header fields
TlvHeader = namedtuple('TlvHeader', 'magic dlen dhash version tag hashtype hhash')
VersionID = namedtuple('VersionID', 'bucket object version')
Block = namedtuple('Block', 'versionid data')
Range = namedtuple('Range', 'start len')
PackEntry = namedtuple('PackEntry', 'packid sourcerange packrange blocklens sourcelens')
PackList = namedtuple('PackList', 'versionid uploadid packs')


def read_tlv_header(f: typing.BinaryIO) -> TlvHeader:
    """
    Read, validate, and decode one TLV header from a file-like IO, leaving
    the IO positioned at the start of the value.
    :param f: file-like IO to read from
    :return: decoded TLV header tuple
    """
    header_raw = f.read(32)

    if len(header_raw) == 0:
        raise EOFError

    if len(header_raw) < 32:
        raise RuntimeError(f'TLV header too short; need 32 bytes, got {len(header_raw)}')

    h = TlvHeader._make(unpack("!8sQQB2sBxxH", header_raw))

    if h.magic != b'\x89TLV\r\n\x1a\n':
        raise 'invalid TLV header magic'

    if h.version != 0:
        raise f'unknown version {h.version}; can only handle TLV version 0'

    if h.hashtype != 8:
        raise f'invalid hash type {h.hashtype}; can only handle 8 (xxhash64)'

    if h.hhash != (xxh64(header_raw[0:30]).intdigest() % 2 ** 16):
        raise 'TLV header hash mismatch'

    return h


def read_tlv(f: typing.BinaryIO) -> (TlvHeader, bytes):
    """
    Read a complete TLV from the file-like IO, leaving the IO positioned at the start of the next TLV.
    :param f: file-like IO to read from
    :return: TLV header tuple and value bytes
    """
    header = read_tlv_header(f)
    data = f.read(header.dlen)

    if len(data) < header.dlen:
        raise f'short data read: expected {header.dlen} bytes, got {len(data)}'

    if header.dhash != xxh64(data).intdigest():
        raise "TLV data hash mismatch"

    return header, data


def decode_value(f: typing.BinaryIO, dlen: int) -> (dict, Optional[bytes]):
    """
    Decode a value from a file-like IO, returning the primary part as a dict
    and the secondary part (if present) as raw bytes.
    :param f: file-like IO to read from
    :param dlen: length of the value
    :return: primary value (as dict) and secondary value (bytes or None)
    """
    unpacker = msgpack.Unpacker(f)
    val = unpacker.unpack()

    if 'z' in val:
        raise 'encrypted values not supported'  # TODO: decrypt

    # Decompress encoded value if compressed
    if val.get('c') == 1:
        val['e'] = zstd.decompress(val['e'])

    primary = msgpack.unpackb(val['e'])
    secondary = bytes(0)

    try:
        sec_enc = val['s'][0]   # Encoding specifier of secondary part
        sec_len = sec_enc['l']  # Length of secondary part
        f.seek(dlen - sec_len)  # MsgPack may have read the secondary part already, so seek back to it
        secondary = f.read(sec_len)

        # If secondary encoding specifies compression, or that key is missing and primary encoding specifies
        # compression, then decompress the secondary value.
        if sec_enc.get('c', val.get('c')) == 1:
            secondary = zstd.decompress(secondary)
    except IndexError:
        pass  # no secondary part
    except KeyError:
        pass  # no secondary part

    return primary, secondary


def parse_versionid(version_str: str) -> VersionID:
    """
    Parse a version ID string into a VersionID tuple.
    """
    # First 26 characters is a ULID specifying the version
    version = ULID.from_str(version_str[:26])
    # Remaining characters (after a ':' separator) are bucket/object name
    bucket, object = version_str[27:].split('/', maxsplit=1)
    # Together these form the complete version identifier
    return VersionID(bucket=bucket, object=object, version=version)


def parse_range(range: dict) -> Range:
    """
    Convert dict form of range (from decode_value) to Range tuple.
    """
    return Range(start=range['s'], len=range['l'])


def parse_pack_entry(pack_entry: dict) -> PackEntry:
    """
    Convert dict form of pack entry (from decode_value) to PackEntry tuple.
    """
    return PackEntry(
        packid=ULID.from_str(pack_entry['p']),
        sourcerange=parse_range(pack_entry['o']),
        packrange=parse_range(pack_entry['t']),
        blocklens=pack_entry['E'],
        sourcelens=pack_entry['N'],
    )


def parse_packs(packs: list) -> list:
    """
    Convert list of pack entries to list of PackEntry tuples.
    """
    return [parse_pack_entry(p) for p in packs]


def parse_block(part1: dict, part2: bytes) -> Block:
    """
    Parse a block from decode_value into a Block tuple.
    """
    versionid = parse_versionid(part1['I'])  # Key 'I' is the version ID
    return Block(versionid=versionid, data=part2)


def parse_packlist(part1: dict, part2: Optional[bytes]) -> PackList:
    """
    Parse a pack list from decode_value into a PackList tuple.
    """
    versionid = parse_versionid(part1['I'])  # Key 'I' is the version ID
    uploadid = part1['U']  # Key 'U' is the upload ID (if multipart) or none if single PUT
    packs = parse_packs(part1['P'])
    packlist = PackList(versionid=versionid, uploadid=uploadid, packs=packs)
    return packlist


def parse_version(part1: dict, part2: Optional[bytes]):
    print('version:')
    pprint(part1)
    raise NotImplementedError("can't do versions yet")


def ltfsvof_reader(f: typing.BinaryIO):
    handlers = {b'bk': parse_block,
                b'pl': parse_packlist,
                b'vr': parse_version}

    while True:
        try:
            header, data = read_tlv(f)
            if header.tag not in handlers:
                raise RuntimeError(f'unknown tag {header.tag}; no handler registered')
            part1, part2 = decode_value(io.BytesIO(data), header.dlen)
            entry = handlers[header.tag](part1, part2)
            yield entry
        except EOFError:
            break


class TlvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tlv_data = base64.b64decode("iVRMVg0KGgoAAAAAAAAADuM9tfSfjss2AEMhCAAAuxRkYXRhIGRhdGEgZGF0YQ==")

    def test_read_tlv_header(self):
        with io.BytesIO(self.tlv_data) as f:
            header = read_tlv_header(f)
            self.assertEqual(header.magic, b'\x89TLV\x0d\x0a\x1a\x0a')
            self.assertEqual(header.dlen, 14)
            self.assertEqual(header.hashtype, 8)
            self.assertEqual(header.version, 0)
            self.assertEqual(header.tag, 0x4321)

    def test_read_tlv(self):
        with io.BytesIO(self.tlv_data) as f:
            header, data = read_tlv(f)
            self.assertEqual(data, b'data data data')

    def test_3tlv(self):
        with open('sample_data/3simple.tlv', 'rb') as f:
            header, data = read_tlv(f)
            self.assertEqual(header.tag, 0x626B)
            self.assertEqual(data, b'data 1')
            header, data = read_tlv(f)
            self.assertEqual(header.tag, 0x626B)
            self.assertEqual(data, b'data 2')
            header, data = read_tlv(f)
            self.assertEqual(header.tag, 0x626B)
            self.assertEqual(data, b'data 3')


class ValueTests(unittest.TestCase):
    def test_value_decode(self):
        with open('sample_data/3values.tlv', 'rb') as f:
            for i in range(3):
                header, data = read_tlv(f)
                part1, part2 = decode_value(io.BytesIO(data), header.dlen)
                self.assertEqual(part1, bytes(f'value {i + 1} header', 'utf-8'))
                self.assertEqual(part2, bytes(f'value {i + 1} data', 'utf-8'))

    def test_compressed_value_decode(self):
        with open('sample_data/compressed_value.tlv', 'rb') as f:
            header, data = read_tlv(f)
            part1, part2 = decode_value(io.BytesIO(data), header.dlen)
            self.assertEqual(part1, b'header header header header header header header header')
            self.assertEqual(part2, b'data data data data data data data data data data data')


class BlockTests(unittest.TestCase):
    def test_read_block(self):
        with open('sample_data/7YGGZJ4YR0R4C0ZACA24BAB17Q.blk', 'rb') as f:
            for entry in ltfsvof_reader(f):
                pprint(entry)


if __name__ == '__main__':
    unittest.main()
