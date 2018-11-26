using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    public interface ISnapShot : IDisposable
    {
        /// <summary>
        /// 得到数据高度
        /// </summary>
        /// <returns></returns>
        UInt64 DataHeight
        {
            get;
        }
        byte[] DataHeightBuf
        {
            get;
        }
        byte[] GetValueData(byte[] tableid, byte[] key);
        DBValue GetValue(byte[] tableid, byte[] key);
        IKeyFinder CreateKeyFinder(byte[] tableid, byte[] beginkey = null, byte[] endkey = null);
        IKeyIterator CreateKeyIterator(byte[] tableid, byte[] _beginkey = null, byte[] _endkey = null);
        byte[] GetTableInfoData(byte[] tableid);
        TableInfo GetTableInfo(byte[] tableid);
        uint GetTableCount(byte[] tableid);
    }
    public interface IKeyIterator : IEnumerator<byte[]>
    {
        UInt64 HandleID
        {
            get;
        }
    }
    public interface IKeyFinder : IEnumerable<byte[]>
    {

    }
}
