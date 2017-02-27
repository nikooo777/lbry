import os
import logging
import sqlite3
from twisted.internet import defer
from twisted.enterprise import adbapi
from zope.interface import implements
from lbrynet.interfaces import IStorage
from lbrynet.core import utils
from lbrynet.core import Error
from lbrynet import conf

log = logging.getLogger(__name__)


class STREAM_STATUS(object):
    RUNNING = "running"
    STOPPED = "stopped"
    FINISHED = "finished"
    PENDING = "pending"


class CLAIM_STATUS(object):
    INIT = "INIT"
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    INVALID_METADATA = "INVALID_METADATA"


class MemoryStorage(object):
    implements(IStorage)

    def __init__(self):
        self.db_path = ":MEMORY:"
        self.sqlite_db = None
        self._is_open = False

    @property
    def is_open(self):
        return self._is_open is True

    @defer.inlineCallbacks
    def open(self):
        if not self.is_open:
            yield self._open()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def close(self):
        if self.is_open:
            self._is_open = False
            yield self.sqlite_db.close()
        defer.returnValue(True)

    @defer.inlineCallbacks
    def query(self, query, args=None):
        if not self.is_open:
            yield self.open()
        query_str = query.replace("?", "%s")
        if args:
            query_str %= args
        try:
            if args:
                result = yield self.sqlite_db.runQuery(query, args)
            else:
                result = yield self.sqlite_db.runQuery(query)
        except sqlite3.IntegrityError as err:
            log.warning(err)
            result = None
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _open(self):
        log.info("Opening database: %s", self.db_path)
        self.sqlite_db = adbapi.ConnectionPool("sqlite3", self.db_path, check_same_thread=False)
        create_table_queries = [
            ("CREATE TABLE IF NOT EXISTS claims ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "name TEXT NOT NULL, "
             "status TEXT NOT NULL,"
             "txid TEXT NOT NULL, "
             "nout INTEGER, "
             "claim_transaction_id TEXT NOT NULL, "
             "claim_hash TEXT NOT NULL UNIQUE, "
             "sd_blob_id TEXT, "
             "is_mine BOOLEAN "
             ")"),

            ("CREATE TABLE IF NOT EXISTS winning_claims ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "name TEXT NOT NULL UNIQUE, "
             "claim_id INTEGER NOT NULL UNIQUE, "
             "last_checked INTEGER, "
             "FOREIGN KEY(claim_id) REFERENCES claims(id) "
             "ON DELETE CASCADE ON UPDATE CASCADE "
             ")"),

            ("CREATE TABLE IF NOT EXISTS metadata ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "value BLOB,"
             "FOREIGN KEY(id) REFERENCES claims(id) "
             "ON DELETE CASCADE ON UPDATE CASCADE "
             ")"),

            ("CREATE TABLE IF NOT EXISTS files ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "status TEXT NOT NULL,"
             "blob_data_rate REAL, "
             "stream_hash TEXT UNIQUE, "
             "sd_blob_id INTEGER, "
             "decryption_key TEXT, "
             "published_file_name TEXT, "
             "claim_id INTEGER, "
             "FOREIGN KEY(claim_id) REFERENCES claims(id) "
             "ON DELETE SET NULL ON UPDATE CASCADE "
             "FOREIGN KEY(sd_blob_id) REFERENCES blobs(id) "
             "ON DELETE CASCADE ON UPDATE CASCADE)"),

            ("CREATE TABLE IF NOT EXISTS stream_terminators ("
             "id INTEGER PRIMARY KEY, "
             "blob_count INTEGER NOT NULL, "
             "iv TEXT, "
             "FOREIGN KEY(id) REFERENCES files(id) "
             "ON DELETE CASCADE ON UPDATE CASCADE)"
             ),

            ("CREATE TABLE IF NOT EXISTS blobs ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "blob_hash TEXT UNIQUE NOT NULL"
             ")"),

            ("CREATE TABLE IF NOT EXISTS managed_blobs ("
             "id INTEGER PRIMARY KEY, "
             "file_id INTEGER, "
             "stream_position INTEGER, "
             "iv TEXT, "
             "blob_length INTEGER, "
             "last_verified_time INTEGER, "
             "last_announced_time INTEGER, "
             "next_announce_time INTEGER, "
             "FOREIGN KEY(file_id) REFERENCES files(id) "
             "ON DELETE set NULL ON UPDATE CASCADE,"
             "FOREIGN KEY(id) REFERENCES blobs(id) "
             "ON DELETE CASCADE ON UPDATE CASCADE"
             ")"),

            ("CREATE TABLE IF NOT EXISTS blob_transfer_history ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT, "
             "blob_id INTEGER NOT NULL, "
             "peer_ip TEXT NOT NULL, "
             "downloaded boolean, "
             "rate REAL NOT NULL,"
             "time INTEGER NOT NULL,"
             "FOREIGN KEY(blob_id) REFERENCES blobs(id) "
             "ON DELETE SET NULL ON UPDATE CASCADE"
             ")")
        ]

        for create_table_query in create_table_queries:
            yield self.sqlite_db.runQuery(create_table_query)
        yield self.sqlite_db.runQuery("pragma foreign_keys=1")
        self._is_open = True
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_claim_row_id(self, claim_hash):
        query_result = yield self.query("SELECT id FROM claims WHERE claim_hash=?",
                                           (claim_hash,))
        row_id = False
        if query_result:
            row_id = query_result[0][0]
        defer.returnValue(row_id)

    @defer.inlineCallbacks
    def get_file_row_id(self, stream_hash):
        query_result = yield self.query("SELECT id FROM files WHERE stream_hash=?",
                                           (stream_hash,))
        row_id = False
        if query_result:
            row_id = query_result[0][0]
        defer.returnValue(row_id)

    @defer.inlineCallbacks
    def get_blob_row_id(self, blob_hash):
        query_result = yield self.query("SELECT id FROM blobs WHERE blob_hash=?",
                                           (blob_hash,))
        blob_id = False
        if query_result:
            blob_id = query_result[0][0]
        if not blob_id and blob_hash:
            yield self.query("INSERT INTO blobs VALUES (NULL, ?)", (blob_hash, ))
            query_result = yield self.query("SELECT id FROM blobs WHERE blob_hash=?",
                                            (blob_hash,))
            blob_id = query_result[0][0]
            add_managed_blob_query = ("INSERT INTO managed_blobs VALUES "
                                      "(?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
            yield self.query(add_managed_blob_query, (blob_id, ))

        defer.returnValue(blob_id)

    # Metadata manager


    @defer.inlineCallbacks
    def delete_stream(self, stream_hash):
        query = "DELETE FROM files WHERE stream_hash=?"
        yield self.query(query, (stream_hash))

    @defer.inlineCallbacks
    def store_stream(self, stream_hash, file_name, decryption_key, published_file_name):
        query = ("INSERT INTO files VALUES (NULL, ?, NULL, ?, NULL, ?, ?, NULL)")
        try:
            yield self.query(query, (STREAM_STATUS.PENDING,
                                             stream_hash,
                                             decryption_key,
                                             published_file_name))
        except sqlite3.IntegrityError:
            raise Error.DuplicateStreamHashError(stream_hash)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_all_streams(self):
        results = yield self.query("SELECT stream_hash FROM files "
                                   "WHERE stream_hash IS NOT NULL")
        streams = []
        if results:
            streams = [r[0] for r in results]
        defer.returnValue(streams)

    @defer.inlineCallbacks
    def get_stream_info(self, stream_hash):
        query = ("SELECT decryption_key, published_file_name, published_file_name FROM files "
                 "WHERE stream_hash=?")
        result = yield self.query(query, (stream_hash,))
        if result:
            defer.returnValue(result[0])
        else:
            raise Error.NoSuchStreamHash(stream_hash)

    @defer.inlineCallbacks
    def check_if_stream_exists(self, stream_hash):
        query = "SELECT stream_hash FROM files WHERE stream_hash=?"
        results = yield self.query(query, (stream_hash,))
        if results:
            defer.returnValue(True)
        else:
            defer.returnValue(False)

    @defer.inlineCallbacks
    def get_blob_num_by_hash(self, stream_hash, blob_hash):
        query = ("SELECT b.position FROM blobs b "
                 "INNER JOIN files f ON f.stream_hash=?"
                 "WHERE b.blob_hash=?")
        results = yield self.query(query, (stream_hash, blob_hash))
        result = None
        if results:
            result = results[0][0]
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_count_for_stream(self, stream_hash):
        file_id = yield self.get_file_row_id(stream_hash)
        query = ("SELECT count(*) FROM managed_blobs WHERE file_id=?")
        blob_count = yield self.query(query, (file_id, ))
        result = 0
        if blob_count:
            result = blob_count[0][0]
        defer.returnValue(result)

    @defer.inlineCallbacks
    def get_blobs_for_stream(self, stream_hash):
        blob_count = yield self.get_count_for_stream(stream_hash)
        file_id = yield self.get_file_row_id(stream_hash)
        query = ("SELECT id, stream_position, iv, blob_length FROM managed_blobs "
                 "WHERE file_id=? AND stream_position=?")
        blob_infos = []
        for n in range(blob_count):
            result = yield self.query(query, (file_id, n))
            if result:
                blob_id, stream_position, iv, blob_length = result[0]
                blob_query = "SELECT blob_hash FROM blobs WHERE id=?"
                b_h = yield self.query(blob_query, (blob_id, ))
                blob_hash = b_h[0][0]
                blob_infos.append((blob_hash, stream_position, iv, blob_length))
        stream_terminator = yield self.get_stream_terminator(file_id)
        if stream_terminator:
            blob_count, iv = stream_terminator
            blob_infos.append((None, blob_count, iv, 0))
        defer.returnValue(blob_infos)

    @defer.inlineCallbacks
    def add_empty_blob(self, file_id, blob_hash, stream_position, iv, length=0):
        assert blob_hash
        blob_id = yield self.get_blob_row_id(blob_hash)
        if not blob_id:
            add_blob_query = "INSERT INTO blobs VALUES (NULL, ?)"
            yield self.query(add_blob_query, (blob_hash, ))
            blob_id = yield self.get_blob_row_id(blob_hash)
        empty_blob_query = ("INSERT INTO managed_blobs VALUES "
                            "(?, ?, ?, ?, ?, NULL, NULL, NULL)")
        yield self.query(empty_blob_query, (blob_id, file_id, stream_position, iv, length))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_stream_terminator(self, file_id):
        stream_terminator = yield self.query("SELECT blob_count, iv FROM stream_terminators WHERE id=?",
                                             (file_id, ))
        result = False
        if stream_terminator:
            result = stream_terminator[0]
        defer.returnValue(result)

    @defer.inlineCallbacks
    def add_stream_terminator(self, file_id, length, iv):
        has_terminator = yield self.get_stream_terminator(file_id)
        query = "INSERT INTO stream_terminators VALUES (?, ?, ?)"
        if has_terminator is False:
            yield self.query(query, (file_id, length, iv))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def add_blobs_to_stream(self, stream_hash, blobs, ignore_duplicate_error=False):
        update_blob_query = ("UPDATE managed_blobs SET "
                                   "file_id=?, "
                                   "stream_position=?, "
                                   "iv=?, "
                                   "blob_length=? "
                               "WHERE id=?")

        file_id = yield self.get_file_row_id(stream_hash)

        for blob in blobs:
            if blob.blob_hash is None and blob.length == 0:
                yield self.add_stream_terminator(file_id, blob.blob_num, blob.iv)
            else:
                blob_id = yield self.get_blob_row_id(blob.blob_hash)
                yield self.query(update_blob_query, (file_id, blob.blob_num, blob.iv, blob.length,
                                                     blob_id))

        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_stream_of_blobhash(self, blob_hash):
        query = ("SELECT f.stream_hash FROM files f "
                 "INNER JOIN managed_blobs mb ON mb.file_id=f.id "
                 "INNER JOIN blobs b ON mb.id=b.id "
                 "WHERE b.blob_hash=? ")
        results = yield self.query(query, (blob_hash,))
        result = None
        if results:
            result = results[0]
        defer.returnValue(result)

    @defer.inlineCallbacks
    def save_sd_blob_hash_to_stream(self, stream_hash, sd_blob_hash):
        file_id = yield self.get_file_row_id(stream_hash)
        sd_blob_id = yield self.get_blob_row_id(sd_blob_hash)
        update_files = ("UPDATE files SET "
                        "sd_blob_id=? "
                         "WHERE id=?")
        update_blobs = ("UPDATE managed_blobs SET "
                        "file_id=? "
                        "WHERE id=?")
        yield self.query(update_files, (sd_blob_id, file_id))
        yield self.query(update_blobs, (file_id, sd_blob_id))

        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_sd_hash_for_stream(self, stream_hash):
        file_id = yield self.get_file_row_id(stream_hash)
        sd_blob_id = yield self.query("SELECT sd_blob_id FROM files WHERE id=?", (file_id, ))
        results = []
        if sd_blob_id is not None:
            sd_hash = yield self.query("SELECT blob_hash FROM blobs WHERE id=?", sd_blob_id[0])
            if sd_hash:
                results = [sd_hash[0][0]]
        defer.returnValue(results)

    ############# File manager


    @defer.inlineCallbacks
    def save_lbry_file(self, stream_hash, data_payment_rate):
        log.info("Save new file")
        rowid = yield self.get_file_row_id(stream_hash)
        if data_payment_rate is None:
            data_payment_rate = 0.0
        if not rowid:
            yield self.query("INSERT INTO files VALUES "
                                     "(NULL, ?, ?, ?, NULL, NULL, NULL, NULL)",
                                     (STREAM_STATUS.PENDING,
                                      data_payment_rate, stream_hash))
        yield self.get_file_row_id(stream_hash)

    @defer.inlineCallbacks
    def delete_lbry_file_options(self, rowid):
        yield self.query("DELETE FROM files WHERE id=?", (rowid,))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def set_lbry_file_payment_rate(self, rowid, new_rate):
        yield self.query("UPDATE files SET blob_data_rate=? where id=?",
                                  (new_rate, rowid))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_all_lbry_files(self):
        results = yield self.query("SELECT id, stream_hash, blob_data_rate FROM files")
        defer.returnValue(results)

    @defer.inlineCallbacks
    def change_file_status(self, rowid, new_status):
        yield self.query("UPDATE files SET status=? WHERE id=?", (new_status, rowid))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_lbry_file_status(self, rowid):
        query_string = "SELECT status FROM files WHERE id=?"
        query_results = yield self.query(query_string, (rowid,))
        status = None
        if query_results:
            status = query_results[0][0]
        defer.returnValue(status)


    ################ Blob manager

    @defer.inlineCallbacks
    def add_completed_blob(self, blob_hash, length, next_announce_time):
        blob_id = yield self.get_blob_row_id(blob_hash)
        query = "UPDATE managed_blobs SET blob_length=?, next_announce_time=? WHERE id=?"
        yield self.query(query, (length, next_announce_time, blob_id))
        yield self.update_blob_verified_timestamp(blob_hash, utils.time())
        defer.returnValue(None)

    @defer.inlineCallbacks
    def update_blob_verified_timestamp(self, blob_hash, timestamp):
        blob_id = yield self.get_blob_row_id(blob_hash)
        query = "UPDATE managed_blobs SET last_verified_time=? WHERE id=?"
        yield self.query(query, (timestamp, blob_id))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_blobs_to_announce(self):
        timestamp = int(utils.time())
        query = ("SELECT blob_hash FROM blobs "
                     "INNER JOIN managed_blobs mb "
                         "ON mb.id=blobs.id AND "
                             "mb.next_announce_time<? AND "
                             "blobs.blob_hash IS NOT NULL")
        blob_hashes = yield self.query(query, (timestamp, ))
        blob_hashes = [r[0] for r in blob_hashes]
        defer.returnValue(blob_hashes)

    @defer.inlineCallbacks
    def update_next_blob_announce(self, blob_hashes, next_announce_time):
        update_query = ("UPDATE managed_blobs SET next_announce_time=? "
                        "WHERE id=? ")
        for blob_hash in blob_hashes:
            id = yield self.get_blob_row_id(blob_hash)
            yield self.query(update_query, (next_announce_time, id))

    @defer.inlineCallbacks
    def delete_blob(self, blob_hash):
        blob_id = yield self.get_blob_row_id(blob_hash)
        yield self.query("DELETE FROM blobs where id=?", (blob_id,))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def get_all_verified_blob_hashes(self, blob_dir=None):
        blob_hashes = yield self.query("SELECT blob_hash FROM blobs")
        verified_blobs = []
        for blob_hash, in blob_hashes:
            if blob_dir is not None:
                file_path = os.path.join(blob_dir, blob_hash)
                if os.path.isfile(file_path):
                    verified_blobs.append(blob_hash)
                    yield self.update_blob_verified_timestamp(blob_hash, utils.now())
            else:
                verified_blobs.append(blob_hash)
                yield self.update_blob_verified_timestamp(blob_hash, utils.now())
        defer.returnValue(verified_blobs)

    @defer.inlineCallbacks
    def add_blob_to_download_history(self, blob_hash, host, rate):
        ts = int(utils.time())
        blob_id = yield self.get_blob_row_id(blob_hash)
        query = "INSERT INTO blob_transfer_history VALUES (NULL, ?, ?, ?, ?, ?) "
        yield self.query(query, (blob_id, str(host), True, float(rate), ts))
        defer.returnValue(None)

    @defer.inlineCallbacks
    def add_blob_to_upload_history(self, blob_hash, host, rate):
        ts = int(utils.time())
        blob_id = yield self.get_blob_row_id(blob_hash)
        query = "INSERT INTO blob_transfer_history VALUES (NULL, ?, ?, ?, ?, ?) "
        yield self.query(query, (blob_id, str(host), False, float(rate), ts))
        defer.returnValue(None)

    ##### Wallet

    def get_claim_hash(self, outpoint):
        return utils.claim_hash(outpoint['txid'], outpoint['nout'])

    @defer.inlineCallbacks
    def add_claim(self, name, txid, nout, claim_id, is_mine=False):
        claim_hash = utils.claim_hash(txid, nout)
        claim_row_id = yield self.get_claim_row_id(claim_hash)
        assert not claim_row_id, Exception("Claim already known")
        query = "INSERT INTO claims VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, NULL)"
        yield self.query(query, (name, CLAIM_STATUS.INIT, txid, nout, claim_id, claim_hash, is_mine))
        defer.returnValue(True)

    @defer.inlineCallbacks
    def add_metadata_to_claim(self, claim_hash, metadata):
        sd_hash = utils.get_sd_hash(metadata)
        claim_row_id = yield self.get_claim_row_id(claim_hash)
        try:
            add_sd_hash_query = "UPDATE claims SET sd_hash=? WHERE id=?"
            yield self.query(add_sd_hash_query, (sd_hash, claim_row_id))

            add_metadata_query = "INSERT INTO metadata VALUES (?, ?)"
            yield self.query(add_metadata_query, (claim_row_id, utils.metadata_to_b58(metadata)))
            status_code = CLAIM_STATUS.PENDING
        except Exception as err:
            status_code = CLAIM_STATUS.INVALID_METADATA
            log.warning(err)
        yield self.update_claim_status(claim_hash, status_code)
        defer.returnValue(status_code)

    @defer.inlineCallbacks
    def update_claim_status(self, claim_hash, status):
        assert status in CLAIM_STATUS, Exception("Unknown status: %s" % status)
        row_id = yield self.get_claim_row_id(claim_hash)
        assert row_id is not False, Exception("No claim to update")
        query = "UPDATE claims SET status=? WHERE id=?"
        yield self.query(query, (status, row_id))
        defer.returnValue(True)

    @defer.inlineCallbacks
    def get_claim_status(self, claim_hash):
        status = None
        row_id = yield self.get_claim_row_id(claim_hash)
        if row_id is not False:
            query = "SELECT status FROM claims WHERE id=?"
            query_result = yield self.query(query, (row_id, ))
            status = query_result[0][0]
        defer.returnValue(status)

    @defer.inlineCallbacks
    def get_metadata_for_claim(self, claim_hash):
        row_id = yield self.get_claim_row_id(claim_hash)
        metadata = None
        if row_id is not False:
            query = "SELECT value FROM metadata WHERE id=?"
            blob = yield self.query(query, (row_id, ))
            encoded_metadata = blob[0][0]
            metadata = utils.decode_b58_metadata(encoded_metadata)
        defer.returnValue(metadata)

    @defer.inlineCallbacks
    def clean_bad_records(self):
        yield self.query("DELETE FROM claims WHERE LENGTH(txid) > 64 OR txid IS NULL")
        defer.returnValue(None)

    @defer.inlineCallbacks
    def save_name_metadata(self, claim_outpoint, metadata):
        claim_hash = self.get_claim_hash(claim_outpoint)
        status = yield self.get_claim_status(claim_hash)

        if not status:
            raise Exception("No such claim")
        if status == CLAIM_STATUS.INIT:
            yield self.add_metadata_to_claim(claim_hash, metadata)
        defer.returnValue(True)

    # def update_claimid(self, claim_id, name, claim_outpoint):
    #     d = self.db.runQuery(
    #         "delete from claim_ids where claimId=? and name=? and txid=? and n=?",
    #         (claim_id, name, claim_outpoint['txid'], claim_outpoint['nout']))
    #     d.addCallback(
    #         lambda _: self.db.runQuery(
    #             "delete from claim_ids where claimId=? and name=? and txid=? and n=?",
    #             (claim_id, name, claim_outpoint['txid'], UNSET_NOUT)))
    #     d.addCallback(
    #         lambda r: self.db.runQuery(
    #             "insert into claim_ids values (?, ?, ?, ?)",
    #             (claim_id, name, claim_outpoint['txid'], claim_outpoint['nout'])))
    #     d.addCallback(lambda _: claim_id)
    #     return d

    @defer.inlineCallbacks
    def get_claimid_for_tx(self, claim_outpoint):
        query = "SELECT claim_transaction_id FROM claims WHERE txid=? and nout=?"
        query_result = yield self.query(query, (claim_outpoint['txid'],
                                                claim_outpoint['nout']))
        claim_id = None
        if query_result:
            claim_id = query_result[0][0]
        defer.returnValue(claim_id)


class FileStorage(MemoryStorage):
    def __init__(self, db_dir=None):
        MemoryStorage.__init__(self)
        self.db_path = os.path.join(db_dir or conf.default_data_dir, "lbry.sqlite")
