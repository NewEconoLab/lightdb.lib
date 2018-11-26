using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LightDB
{
    class TableKeyFinder : IKeyFinder
    {
        public TableKeyFinder(SnapShot _snapshot, byte[] _tableid, byte[] _beginkey, byte[] _endkey)
        {
            this.snapshot = _snapshot;
            this.tableid = _tableid;
            this.beginkeyfinal = Helper.CalcKey(_tableid, _beginkey);
            this.endkeyfinal = Helper.CalcKey(_tableid, _endkey);
        }
        SnapShot snapshot;
        byte[] tableid;
        byte[] beginkeyfinal;
        byte[] endkeyfinal;
        public IEnumerator<byte[]> GetEnumerator()
        {
            return new TableIterator(snapshot, tableid, beginkeyfinal, endkeyfinal);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    class TableIterator : IKeyIterator
    {
        public TableIterator(SnapShot snapshot, byte[] _tableid, byte[] _beginkeyfinal, byte[] _endkeyfinal)
        {
            this.itPtr = RocksDbSharp.Native.Instance.rocksdb_create_iterator(snapshot.dbPtr, snapshot.readopHandle);
            //this.it = snapshot.db.NewIterator(null, snapshot.readop);
            this.tableid = _tableid;
            this.beginkeyfinal = _beginkeyfinal;
            this.endkeyfinal = _endkeyfinal;
            //this.Reset();

        }
        public UInt64 HandleID
        {
            get
            {
                return (UInt64)itPtr.ToInt64();
            }
        }
        bool bInit = false;
        IntPtr itPtr;
        //RocksDbSharp.Iterator it;
        byte[] tableid;
        byte[] beginkeyfinal;
        byte[] endkeyfinal;
        public byte[] Current
        {
            get
            {
                if (this.Vaild)
                {
                    var key = RocksDbSharp.Native.Instance.rocksdb_iter_key(itPtr);
                    return key.Skip(this.tableid.Length + 2).ToArray();
                    //return it.Key().Skip(this.tableid.Length + 2).ToArray();
                }
                else
                    return null;
            }
        }

        object IEnumerator.Current
        {
            get
            {
                return Current;
            }
        }

        public bool Vaild
        {
            get;
            private set;
        }
        public bool TestVaild(byte[] data)
        {
            if (data.Length < this.endkeyfinal.Length)
                return false;
            for (var i = 0; i < endkeyfinal.Length; i++)
            {
                if (data[i] != this.endkeyfinal[i])
                    return false;
            }
            return true;
        }
        public bool MoveNext()
        {
            if (bInit == false)
            {
                bInit = true;
                RocksDbSharp.Native.Instance.rocksdb_iter_seek(itPtr, beginkeyfinal, (ulong)beginkeyfinal.Length);

                // it.Seek(beginkeyfinal);
            }
            else
            {
                RocksDbSharp.Native.Instance.rocksdb_iter_next(itPtr);

                //it.Next();
            }
            if (RocksDbSharp.Native.Instance.rocksdb_iter_valid(itPtr) == false)
                return false;
            var key = RocksDbSharp.Native.Instance.rocksdb_iter_key(itPtr);
            this.Vaild = TestVaild(key);
            return this.Vaild;
        }

        public void Reset()
        {
            RocksDbSharp.Native.Instance.rocksdb_iter_seek(itPtr, beginkeyfinal, (ulong)beginkeyfinal.Length);

            //it.Seek(beginkeyfinal);
            bInit = false;
            this.Vaild = false;
        }

        public void Dispose()
        {
            RocksDbSharp.Native.Instance.rocksdb_iter_destroy(this.itPtr);
            this.itPtr = IntPtr.Zero;
            //it.Dispose();
            //it = null;
        }
    }

}
