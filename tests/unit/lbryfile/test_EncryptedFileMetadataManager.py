from twisted.trial import unittest
from twisted.internet import defer
from lbrynet.lbryfile.EncryptedFileMetadataManager import DBEncryptedFileMetadataManager
from lbrynet.core import utils
from lbrynet.cryptstream.CryptBlob import CryptBlobInfo
from lbrynet.core.Error import NoSuchStreamHash
from tests.util import random_lbry_hash

class DBEncryptedFileMetadataManagerTest(unittest.TestCase):
    def setUp(self):
        pass

    @defer.inlineCallbacks
    def test_basic(self):
        db_dir='.'
        manager = DBEncryptedFileMetadataManager(db_dir)
        yield manager.setup()
        out = yield manager.get_all_streams()
        self.assertEqual(len(out),0)

        stream_hash =  random_lbry_hash()
        file_name = 'file_name'
        key = 'key'
        suggested_file_name = 'sug_file_name'
        blob1 = CryptBlobInfo(random_lbry_hash(),0,10,1)
        blob2 = CryptBlobInfo(random_lbry_hash(),0,10,1)
        blobs=[blob1,blob2]

        # save stream
        yield manager.save_stream(stream_hash, file_name, key, suggested_file_name, blobs)

        out = yield manager.get_stream_info(stream_hash)
        self.assertEqual(key, out[0])
        self.assertEqual(file_name, out[1])
        self.assertEqual(suggested_file_name, out[2])

        out = yield manager.check_if_stream_exists(stream_hash)
        self.assertTrue(out)

        out = yield manager.get_blobs_for_stream(stream_hash)
        self.assertEqual(2, len(out))

        out = yield manager.get_all_streams()
        self.assertEqual(1, len(out))

        # add a blob to stream
        blob3 = CryptBlobInfo(random_lbry_hash(),0,10,1)
        blobs = [blob3]
        out = yield manager.add_blobs_to_stream(stream_hash,blobs)
        out = yield manager.get_blobs_for_stream(stream_hash)
        self.assertEqual(3, len(out))

        out = yield manager.get_stream_of_blob(blob3.blob_hash)
        self.assertEqual(stream_hash, out)

        # check non existing stream
        with self.assertRaises(NoSuchStreamHash):
            out = yield manager.get_stream_info(random_lbry_hash())

        # check save of sd blob hash
        sd_blob_hash = random_lbry_hash()
        yield manager.save_sd_blob_hash_to_stream(stream_hash, sd_blob_hash)
        out = yield manager.get_sd_blob_hashes_for_stream(stream_hash)
        self.assertEqual(1, len(out))
        self.assertEqual(sd_blob_hash,out[0])

        out = yield manager.get_stream_hash_for_sd_hash(sd_blob_hash)
        self.assertEqual(stream_hash, out)



    

