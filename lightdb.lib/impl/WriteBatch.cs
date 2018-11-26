using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    //public interface IWriteBatchFinal
    //{
    //    void PutDataFinal(byte[] finalkey, byte[] value);
    //    void DeleteFinal(byte[] finalkey);
    //}
    public interface IWriteBatch
    {
        ISnapShot snapshot
        {
            get;
        }
        byte[] GetDataFinal(byte[] finalkey);
        void CreateTable(TableInfo info);
        void CreateTable(byte[] tableid, byte[] finaldata);
        void DeleteTable(byte[] tableid, bool makeTag = false);
        void PutUnsafe(byte[] tableid, byte[] key, byte[] finaldata);
        void Put(byte[] tableid, byte[] key, DBValue value);
        void Delete(byte[] tableid, byte[] key, bool makeTag = false);
    }
    /// <summary>
    /// WriteBatch 写入批，是个很基本的功能，不应该对外暴露
    /// </summary>

    class WriteBatch : IWriteBatch, IDisposable
    {
        public WriteBatch(IntPtr dbptr, SnapShot snapshot)
        {
            this.dbPtr = dbptr;
            this.batchptr = RocksDbSharp.Native.Instance.rocksdb_writebatch_create();
            //this.batch = new RocksDbSharp.WriteBatch();
            this._snapshot = snapshot;
            this.cache = new Dictionary<string, byte[]>();
        }
        //RocksDbSharp.RocksDb db;
        public IntPtr dbPtr;
        public SnapShot _snapshot;
        public ISnapShot snapshot
        {
            get
            {
                return _snapshot;
            }
        }
        //public RocksDbSharp.WriteBatch batch;
        public IntPtr batchptr;
        Dictionary<string, byte[]> cache;

        public void Dispose()
        {
            if (batchptr != IntPtr.Zero)
            {
                RocksDbSharp.Native.Instance.rocksdb_writebatch_destroy(batchptr);
                batchptr = IntPtr.Zero;
                //batch.Dispose();
                //batch = null;
            }
        }
        public byte[] GetDataFinal(byte[] finalkey)
        {
            var hexkey = finalkey.ToString_Hex();
            if (cache.ContainsKey(hexkey))
            {
                return cache[hexkey];
            }
            else
            {
                var data = RocksDbSharp.Native.Instance.rocksdb_get(dbPtr, _snapshot.readopHandle, finalkey);
                if (data == null || data.Length == 0)
                    return null;
                //db.Get(finalkey, null, snapshot.readop);
                cache[hexkey] = data;
                return data;
            }
        }
        private void PutDataFinal(byte[] finalkey, byte[] value)
        {
            var hexkey = finalkey.ToString_Hex();
            cache[hexkey] = value;
            RocksDbSharp.Native.Instance.rocksdb_writebatch_put(batchptr, finalkey, (ulong)finalkey.Length, value, (ulong)value.Length);
            //batch.Put(finalkey, value);
        }
        private void DeleteFinal(byte[] finalkey)
        {
            var hexkey = finalkey.ToString_Hex();
            cache.Remove(hexkey);
            RocksDbSharp.Native.Instance.rocksdb_writebatch_delete(batchptr, finalkey, (ulong)finalkey.Length);
            //batch.Delete(finalkey);
        }
        public void CreateTable(TableInfo info)
        {
            var finalkey = Helper.CalcKey(info.tableid, null, SplitWord.TableInfo);
            var countkey = Helper.CalcKey(info.tableid, null, SplitWord.TableCount);
            var data = GetDataFinal(finalkey);
            if (data != null && data[0] != (byte)DBValue.Type.Deleted)
            {
                throw new Exception("alread have that.");
            }
            var value = DBValue.FromValue(DBValue.Type.Bytes, info.ToBytes());
            value.lastHeight = _snapshot.DataHeight;
            PutDataFinal(finalkey, value.ToBytes(true));

            DBValue count = DBValue.FromRaw(GetDataFinal(countkey));
            if (count == null)
            {
                count = DBValue.FromValue(DBValue.Type.UINT32, (UInt32)0);
                count.lastHeight = _snapshot.DataHeight;
            }
            PutDataFinal(countkey, count.ToBytes(true));
        }
        public void CreateTable(byte[] tableid, byte[] finaldata)
        {
            var finalkey = Helper.CalcKey(tableid, null, SplitWord.TableInfo);
            var countkey = Helper.CalcKey(tableid, null, SplitWord.TableCount);
            var data = GetDataFinal(finalkey);
            if (data != null && data[0] != (byte)DBValue.Type.Deleted)
            {
                throw new Exception("alread have that.");
            }
            //var value = DBValue.FromValue(DBValue.Type.Bytes, infodata);
            PutDataFinal(finalkey, finaldata);

            DBValue count = DBValue.FromRaw(GetDataFinal(countkey));
            if (count == null)
            {
                count = DBValue.FromValue(DBValue.Type.UINT32, (UInt32)0);
                count.lastHeight = _snapshot.DataHeight; 
            }
            PutDataFinal(countkey, count.ToBytes(true));
        }
        public void DeleteTable(byte[] tableid, bool makeTag = false)
        {
            var finalkey = Helper.CalcKey(tableid, null, SplitWord.TableInfo);
            //var countkey = Helper.CalcKey(tableid, null, SplitWord.TableCount);
            var vdata = GetDataFinal(finalkey);
            if (vdata != null && vdata[0] != (byte)DBValue.Type.Deleted)
            {
                if (makeTag)
                {
                    var delete = DBValue.DeletedValue;
                    delete.lastHeight = _snapshot.DataHeight;
                    PutDataFinal(finalkey, delete.ToBytes(true));
                    //PutDataFinal(countkey, DBValue.DeletedValue.ToBytes());
                }
                else
                {
                    DeleteFinal(finalkey);
                    //DeleteFinal(countkey);
                }
            }
            else//数据不存在
            {
                if (makeTag)
                {
                    var delete = DBValue.DeletedValue;
                    delete.lastHeight = _snapshot.DataHeight;
                    PutDataFinal(finalkey, delete.ToBytes(true));
                    //PutDataFinal(countkey, DBValue.DeletedValue.ToBytes());
                }
            }
        }
        public void PutUnsafe(byte[] tableid, byte[] key, byte[] finaldata)
        {
            var finalkey = Helper.CalcKey(tableid, key);
            var countkey = Helper.CalcKey(tableid, null, SplitWord.TableCount);
            var countdata = GetDataFinal(countkey);
            UInt32 count = 0;
            if (countdata != null)
            {
                count = DBValue.FromRaw(countdata).AsUInt32();
            }
            var vdata = GetDataFinal(finalkey);
            if (vdata == null || vdata[0] == (byte)DBValue.Type.Deleted)
            {
                count++;
            }
            else
            {
                if (DBValue.BytesEqualWithoutHeight(vdata, finaldata) == false)
                    count++;
            }
            PutDataFinal(finalkey, finaldata);

            var countvalue = DBValue.FromValue(DBValue.Type.UINT32, count);
            countvalue.lastHeight = _snapshot.DataHeight;
            PutDataFinal(countkey, countvalue.ToBytes(true));
        }

        public void Put(byte[] tableid, byte[] key, DBValue value)
        {
            value.lastHeight = _snapshot.DataHeight;
            PutUnsafe(tableid, key, value.ToBytes(true));
        }
        public void Delete(byte[] tableid, byte[] key, bool makeTag = false)
        {
            var finalkey = Helper.CalcKey(tableid, key);

            var countkey = Helper.CalcKey(tableid, null, SplitWord.TableCount);
            var countdata = GetDataFinal(countkey);
            UInt32 count = 0;
            if (countdata != null)
            {
                count = DBValue.FromRaw(countdata).AsUInt32();
            }

            var vdata = GetDataFinal(finalkey);
            if (vdata != null && vdata[0] != (byte)DBValue.Type.Deleted)
            {
                if (makeTag)
                {
                    var delete = DBValue.DeletedValue;
                    delete.lastHeight = _snapshot.DataHeight;
                    PutDataFinal(finalkey, delete.ToBytes(true));
                }
                else
                {
                    DeleteFinal(finalkey);
                }
                count--;
                var countvalue = DBValue.FromValue(DBValue.Type.UINT32, count);
                countvalue.lastHeight = _snapshot.DataHeight;
                PutDataFinal(countkey, countvalue.ToBytes(true));
            }
            else//数据不存在
            {
                if (makeTag)
                {
                    var delete = DBValue.DeletedValue;
                    delete.lastHeight = _snapshot.DataHeight;
                    PutDataFinal(finalkey, delete.ToBytes(true));
                }
            }

        }
    }


}
