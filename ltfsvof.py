from __future__ import annotations

import base64
import io
import msgpack
import typing
import unittest
import zstd
from pprint import pprint
from ulid import ULID
from struct import unpack
from typing import Optional
from xxhash import xxh64


# TODO: add support for encrypted values
# TODO: version delete structure

class TlvHeader:
    def __init__(self, f: typing.BinaryIO):
        buf = f.read(32)

        if len(buf) == 0:
            raise EOFError

        if len(buf) < 32:
            raise RuntimeError(f'TLV header too short; need 32 bytes, got {len(buf)}')

        self.magic, self.dlen, self.dhash, self.version, \
            self.tag, self.hashtype, self.hhash = unpack("!8sQQB2sBxxH", buf)

        if self.magic != b'\x89TLV\r\n\x1a\n':
            raise 'invalid TLV header magic'

        if self.version != 0:
            raise f'unknown version {self.version}; can only handle TLV version 0'

        if self.hashtype != 8:
            raise f'invalid hash type {self.hashtype}; can only handle 8 (xxhash64)'

        if self.hhash != (xxh64(buf[0:30]).intdigest() % 2 ** 16):
            raise 'TLV header hash mismatch'

    def __repr__(self):
        return f'TlvHeader(tag {self.tag}, dlen {self.dlen})'


class TlvSimple:
    """
    Simple form of TLV that consumes a TLV from a stream, validates its integrity, but
    does not do any decoding on the value.
    """

    def __init__(self, f: typing.BinaryIO):
        """
        Creates a TLV from input stream f, leaving the stream positioned at the start of the next TLV.
        :param f: any file-like stream.
        """
        self.header = TlvHeader(f)
        self.data = f.read(self.header.dlen)

        if len(self.data) < self.header.dlen:
            raise f'short data read: expected {self.header.dlen} bytes, got {len(self.data)}'

        if self.header.dhash != xxh64(self.data).intdigest():
            raise "TLV data hash mismatch"

    def __repr__(self):
        return f'TLV (tag {self.header.tag}, dlen {len(self.data)})'


class TLV:
    """
    TLV reader that reads a TLV from a stream, validates its integrity, and decodes the value.
    """

    def __init__(self, f: typing.BinaryIO):
        """
        Creates a TLV from input stream f, leaving the stream positioned at the start of the next TLV.
        :param f: any file-like stream.
        """
        self.header = TlvHeader(f)
        data = f.read(self.header.dlen)

        if len(data) < self.header.dlen:
            raise f'short data read: expected {self.header.dlen} bytes, got {len(data)}'

        if self.header.dhash != xxh64(data).intdigest():
            raise "TLV data hash mismatch"

        self.__decode_value(data)

    def __decode_value(self, data: bytes):
        """
        Decode a value from self.data, setting the primary part as self.value
        and the secondary part (if present) as self.secondary. self.value
        will be a dict and self.secondary will be bytes or None.
        """
        # Use streaming unpacker because extra data may be present
        unpacker = msgpack.Unpacker(io.BytesIO(data), use_list=False)
        val: dict = unpacker.unpack()

        if 'z' in val:
            # TODO: take apart z to obtain key properties, consult KMS for key, decrypt
            raise NotImplementedError('encrypted values not supported')

        # Decompress encoded value if compressed
        if val.get('c') == 1:
            val['e'] = zstd.decompress(val['e'])

        self.value: dict = msgpack.unpackb(val['e'], use_list=False)
        self.secondary: bytes = bytes(0)

        try:
            sec_enc = val['s'][0]  # Encoding specifier of secondary part
            sec_len = sec_enc['l']  # Length of secondary part
            self.secondary = data[len(data) - sec_len:]

            # If secondary encoding specifies compression, or that key is missing and primary encoding specifies
            # compression, then decompress the secondary value.
            if sec_enc.get('c', val.get('c')) == 1:
                self.secondary = zstd.decompress(self.secondary)
        except IndexError:
            pass  # no secondary part
        except KeyError:
            pass  # no secondary part

    def __repr__(self):
        return f'TLV (tag {self.header.tag})'


class VersionID:
    """
    VersionID represents a LTFS-VOF composite version identifier.
    """

    def __init__(self, bucket: str, object: str, version: str):
        self.bucket: str = bucket
        self.object: str = object
        self.version: str = version

    def __repr__(self):
        return f'VersionID({self.bucket}, {self.object}, {self.version})'

    @classmethod
    def from_str(cls, v_str: str) -> VersionID:
        """
        Parse a version string into a VersionID.
        """
        # First 26 characters is a ULID specifying the version
        version = ULID.from_str(v_str[:26])
        # Remaining characters (after a ':' separator) are bucket/object name
        bucket, object = v_str[27:].split('/', maxsplit=1)
        # Together these form the complete version identifier
        return cls(bucket=bucket, object=object, version=version)

    @classmethod
    def from_dict(cls, v_dict: dict) -> VersionID:
        """
        Convert dict form of version to VersionID.
        """
        return cls(v_dict['b'], v_dict['o'], ULID.from_str(v_dict['v']))


class Block:
    """
    Block represents a single block of object data.
    """

    def __init__(self, tlv: TLV):
        self.versionid: VersionID = VersionID.from_str(tlv.value['I'])
        self.data: bytes = tlv.secondary

    def __repr__(self):
        return f'Block({self.versionid}, {len(self.data)} bytes)'


class Range:
    """
    Range simply stores a start offset and length.
    """

    def __init__(self, r_dict: dict):
        self.start: int = r_dict.get('s', 0)
        self.len: int = r_dict.get('l', 0)

    def __repr__(self):
        return f'Range(start {self.start}, len {self.len})'


class PackEntry:
    def __init__(self, pe_dict: dict):
        self.packid: ULID = ULID.from_str(pe_dict['p'])
        self.sourcerange: Range = Range(pe_dict['o'])
        self.packrange: Range = Range(pe_dict['t'])
        self.blocklens: list[int] = pe_dict.get('E', [])
        self.sourcelens: list[int] = pe_dict.get('N', [])

    def __repr__(self):
        return f'PackEntry({self.packid}, src {self.sourcerange}, pack {self.packrange})'


class Packs(list):
    """
    List type defined so that can easily tell the difference between a pack reference and a list[PackEntry].
    """

    def __repr__(self):
        return f'Packs({super().__repr__()})'


class PackList:
    """
    PackList is a stored map of one version's list of packs storing that version's data.
    """

    def __init__(self, tlv: TLV):
        self.versionid: VersionID = VersionID.from_str(tlv.value['I'])
        self.uploadid: str = tlv.value.get('U', '')
        self.packs: Packs = Packs([PackEntry(pe_dict) for pe_dict in tlv.value.get('P', [])])

    def __repr__(self):
        return f'PackList({self.versionid}, {len(self.packs)} PackEntry)'


class ACL:
    """
    ACL represents a single ACL entry.
    """

    def __init__(self, acl_dict: dict):
        self.idtype: int = acl_dict['t']  # 0: user, 1: group
        self.id: str = acl_dict['i']  # user/group ID
        self.permissions: int = acl_dict['p']  # 1: read, 2: write, 4: read acl, 8: write acl

    def __repr__(self):
        return f'ACL({self.idtype}, {self.id}, {self.permissions})'


class CryptData:
    """
    CryptData represents the encryption metadata for an object.
    """

    def __init__(self, cd_dict: dict):
        self.type: int = cd_dict['x']  # 0: none, 1: customer managed key, 2: S3 managed key
        self.datakey: bytes = cd_dict['k']  # encrypted data key or MD5 of customer key
        self.extra: bytes = cd_dict['e']  # extra string data

    def __repr__(self):
        return f'CryptData({self.type}, {self.datakey}, {self.extra})'


class PackReference:
    """
    PackReference represents a reference to a pack.
    """

    def __init__(self, pr_dict: dict):
        self.pack: str = pr_dict['k']
        self.packrange: Range = Range(pr_dict['r'])

    def __repr__(self):
        return f'PackReference({self.pack}, {self.packrange})'


class Clone:
    """
    Clone represents a single clone of an object.
    """

    def __init__(self, clone_dict: dict):
        data = clone_dict['l']
        try:
            # see if this is a pack list
            ref = msgpack.unpackb(data)
            if 'p' in ref:
                data = Packs([PackEntry(p) for p in ref['p']])
            elif 'R' in ref:
                data = PackReference(ref['R'])
        except msgpack.FormatError:
            # must not be msgpack, so leave data field as-is
            pass

        self.pool: str = clone_dict['p']
        self.data: Packs | PackReference | bytes = data
        self.flags: int = clone_dict.get('f', 0)
        self.blocklen: int = clone_dict['B']
        self.len: int = clone_dict['s']

    def __repr__(self):
        return f'Clone({self.pool}, {self.data}, {self.flags}, {self.blocklen}, {self.len})'


class Version:
    """
    Version represents a single version of an object.
    """

    def __init__(self, tlv: TLV):
        val = tlv.value
        self.versionid: VersionID = VersionID.from_dict(val)
        self.owner: str = val.get('w', '')
        self.acls: list[ACL] = [ACL(a) for a in val.get('A', [])]
        self.len: int = val.get('l', 0)
        self.etag: str = val.get('e', '')
        self.deletemarker: bool = val.get('d', False)
        self.nullversion: bool = val.get('N', False)
        self.crypt: Optional[CryptData] = CryptData(val['c']) if 'c' in val else None
        self.clones: list[Clone] = [Clone(c) for c in val.get('p', [])]
        self.metadata: dict[str, str] = val.get('s', {})
        self.usermetadata: dict[str, str] = val.get('m', {})
        self.legalhold: bool = val.get('h', False)
        self.data: bytes = val.get('D', b'')

    def __repr__(self):
        return f'Version(id {self.versionid})'


class VersionDelete:
    """
    VersionDelete represents the deletion of a single verison.
    """

    def __init__(self, tlv: TLV):
        # TODO: update for version delete structure once tags are known
        self.versionid: VersionID = VersionID.from_dict(tlv.value)
        self.deleteid: VersionID = VersionID.from_str(tlv.value['???'])

    def __repr__(self):
        return f'VersionDelete(id {self.versionid}, deleteid {self.deleteid})'


def data_pack_reader(f: typing.BinaryIO):
    """
    Scan a data pack file, yielding each entry as a Block or PackList tuple.
    :param f: TLV-encoded data pack file
    """
    handlers = {b'bk': Block, b'ol': PackList}

    while True:
        try:
            tlv = TLV(f)
            if tlv.header.tag not in handlers:
                raise RuntimeError(f'unknown tag {tlv.header.tag}; no handler registered')

            yield handlers[tlv.header.tag](tlv)
        except EOFError:
            break


def ltfsvof_reader(f: typing.BinaryIO):
    """
    Scan any LTFS-VOF file, yielding each entry as tuple of the appropriate type.
    :param f: file-like stream with TLV-encoded blocks or versions
    """
    handlers = {b'bk': Block, b'ol': PackList, b'vm': Version, b'vd': VersionDelete}

    while True:
        try:
            tlv = TLV(f)
            if tlv.header.tag not in handlers:
                raise RuntimeError(f'unknown tag {tlv.header.tag}; no handler registered')

            yield handlers[tlv.header.tag](tlv)
        except EOFError:
            break


class TlvTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tlv_data = base64.b64decode("iVRMVg0KGgoAAAAAAAAADuM9tfSfjss2AEMhCAAAuxRkYXRhIGRhdGEgZGF0YQ==")

    def test_read_tlv_header(self):
        with io.BytesIO(self.tlv_data) as f:
            header = TlvHeader(f)
            self.assertEqual(header.magic, b'\x89TLV\x0d\x0a\x1a\x0a')
            self.assertEqual(header.dlen, 14)
            self.assertEqual(header.hashtype, 8)
            self.assertEqual(header.version, 0)
            self.assertEqual(header.tag, b'C!')

    def test_read_tlv(self):
        with io.BytesIO(self.tlv_data) as f:
            tlv = TlvSimple(f)
            self.assertEqual(tlv.data, b'data data data')

    def test_3tlv(self):
        with open('sample_data/3simple.tlv', 'rb') as f:
            tlv = TlvSimple(f)
            self.assertEqual(tlv.header.tag, b'bk')
            self.assertEqual(tlv.data, b'data 1')
            tlv = TlvSimple(f)
            self.assertEqual(tlv.header.tag, b'bk')
            self.assertEqual(tlv.data, b'data 2')
            tlv = TlvSimple(f)
            self.assertEqual(tlv.header.tag, b'bk')
            self.assertEqual(tlv.data, b'data 3')


class ValueTests(unittest.TestCase):
    def test_value_decode(self):
        with open('sample_data/3values.tlv', 'rb') as f:
            for i in range(3):
                tlv = TLV(f)
                self.assertEqual(tlv.value, bytes(f'value {i + 1} header', 'utf-8'))
                self.assertEqual(tlv.secondary, bytes(f'value {i + 1} data', 'utf-8'))

    def test_compressed_value_decode(self):
        with open('sample_data/compressed_value.tlv', 'rb') as f:
            tlv = TLV(f)
            self.assertEqual(tlv.value, b'header header header header header header header header')
            self.assertEqual(tlv.secondary, b'data data data data data data data data data data data')


class BlockTests(unittest.TestCase):
    def test_read_block(self):
        blocks = []
        packlist = None

        # Sample file contains 3 simple blocks and a packlist
        with open('sample_data/3blocks.blk', 'rb') as f:
            for entry in data_pack_reader(f):
                pprint(entry)
                if isinstance(entry, Block):
                    blocks.append(entry)
                else:
                    packlist = entry

        self.assertEqual(len(blocks), 3)
        self.assertEqual(blocks[0].data, b'block 1 data')
        self.assertEqual(blocks[1].data, b'block 2 data')
        self.assertEqual(blocks[2].data, b'block 3 data')
        self.assertEqual(1, len(packlist.packs))
        self.assertEqual(0, packlist.packs[0].sourcerange.start)
        self.assertEqual(3 * len(b'block x data'), packlist.packs[0].sourcerange.len)

        # Furthermore, we can read individual blocks based on the packlist
        with open('sample_data/3blocks.blk', 'rb') as f:
            packentry = packlist.packs[0]

            # Block 1 will be at the start of the pack range
            f.seek(packentry.sourcerange.start)
            block1 = Block(TLV(f))
            self.assertEqual(block1.data, b'block 1 data')

            # Block 2 will be start of pack range plus the first blocklen
            f.seek(packentry.sourcerange.start + packentry.blocklens[0])
            block2 = Block(TLV(f))
            self.assertEqual(block2.data, b'block 2 data')

            # Block 3 will be start of pack range plus the first two blocklens
            f.seek(packentry.sourcerange.start + packentry.blocklens[0] + packentry.blocklens[1])
            block3 = Block(TLV(f))
            self.assertEqual(block3.data, b'block 3 data')


class VersionTests(unittest.TestCase):
    def test_read_version(self):
        with open('sample_data/7YF1JH4PP45BYWK21Y7H0YHFYN.ver', 'rb') as f:
            for entry in ltfsvof_reader(f):
                # pprint(entry)

                if isinstance(entry, Version):
                    v: Version = entry
                    if isinstance(v.clones[0].data, Packs):
                        print('version has embedded packlist')
                        pprint(v.clones[0].data)
                    elif isinstance(v.clones[0].data, PackReference):
                        pr: PackReference = v.clones[0].data
                        print(f'need to load packlist from {pr.pack}')
                        with open(f'sample_data/{pr.pack}.blk', 'rb') as f2:
                            f2.seek(pr.packrange.start)
                            packlist = PackList(TLV(f2))
                            pprint(packlist.packs)


if __name__ == '__main__':
    unittest.main()
