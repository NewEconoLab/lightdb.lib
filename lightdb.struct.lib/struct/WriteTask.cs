using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    public enum WriteTaskOP : byte
    {
        CreateTable,
        DeleteTable,
        PutValue,
        DeleteValue,
        Log,//log 暂时没有作用，仅仅提供存放个信息
    }
    public class WriteTaskItem
    {
        public WriteTaskOP op;
        public byte[] tableID;
        public byte[] key;
        public byte[] value;
        public override string ToString()
        {
            return op.ToString() + " tid=" + tableID?.ToString_Hex() + ",key=" + key?.ToString_Hex() + ",value=" + value?.ToString_Hex();
        }
        public void Pack(System.IO.Stream stream)
        {
            byte lenID = tableID == null ? (byte)0 : (byte)tableID.Length;
            byte lenKey = key == null ? (byte)0 : (byte)key.Length;
            UInt32 lenValue = value == null ? 0 : (UInt32)value.Length;
            stream.WriteByte((byte)op);
            stream.WriteByte(lenID);
            stream.WriteByte(lenKey);
            stream.Write(BitConverter.GetBytes(lenValue), 0, 4);
            if (lenID > 0)
                stream.Write(tableID, 0, lenID);
            if (lenKey > 0)
                stream.Write(key, 0, lenKey);
            if (lenValue > 0)
                stream.Write(value, 0, (int)lenValue);
        }
        public static WriteTaskItem UnPack(System.IO.Stream stream)
        {
            WriteTaskItem item = new WriteTaskItem();
            item.op = (WriteTaskOP)(byte)stream.ReadByte();
            byte lenID = (byte)stream.ReadByte();
            byte lenKey = (byte)stream.ReadByte();
            byte[] bufLenValue = new byte[4];
            stream.Read(bufLenValue, 0, 4);
            UInt32 lenvalue = BitConverter.ToUInt32(bufLenValue, 0);
            item.tableID = new byte[lenID];
            item.key = new byte[lenKey];
            item.value = new byte[lenvalue];
            if (lenID > 0)
                stream.Read(item.tableID, 0, lenID);
            if (lenKey > 0)
                stream.Read(item.key, 0, lenKey);
            if (lenvalue > 0)
                stream.Read(item.value, 0, (int)lenvalue);
            return item;
        }
    }

    public class WriteTask
    {
        /// <summary>
        /// write task 构造不需要啥
        /// </summary>
        public WriteTask()
        {
            items = new List<WriteTaskItem>();
        }

        ///// <summary>
        ///// 增加一个保存附加数据的手段
        ///// </summary>
        public Dictionary<string, byte[]> extData;
        public void AddExtData(byte[] id, byte[] data)
        {
            if (extData == null)
                extData = new Dictionary<string, byte[]>();
            extData[id.ToString_Hex()] = data;
        }
        public List<WriteTaskItem> items;


        public void CreateTable(TableInfo info)
        {
            //donot check in here.
            //if (info.tableid.Length < 2)
            //    throw new Exception("not allow too short table id.");
            items.Add(
                    new WriteTaskItem()
                    {
                        op = WriteTaskOP.CreateTable,
                        tableID = info.tableid,
                        key = null,
                        value = DBValue.FromValue(DBValue.Type.Bytes, info.ToBytes()).ToBytes(false)
                    }
                );
        }
        public void DeleteTable(byte[] tableid)
        {
            items.Add(
                     new WriteTaskItem()
                     {
                         op = WriteTaskOP.DeleteTable,
                         tableID = tableid,
                         key = null,
                         value = null
                     }
                 );
        }
        public void PutUnsafe(byte[] tableid, byte[] key, byte[] data)
        {
            items.Add(
                     new WriteTaskItem()
                     {
                         op = WriteTaskOP.PutValue,
                         tableID = tableid,
                         key = key,
                         value = data
                     }
                 );
        }
        public void Put(byte[] tableid, byte[] key, DBValue value)
        {
            PutUnsafe(tableid, key, value.ToBytes(false));
        }
        public void Delete(byte[] tableid, byte[] key)
        {
            items.Add(
                    new WriteTaskItem()
                    {
                        op = WriteTaskOP.DeleteValue,
                        tableID = tableid,
                        key = key,
                        value = null
                    }
                );
        }
        public void FixHeight(UInt64 height)
        {
            var heightbuf = BitConverter.GetBytes(height);

            foreach (var item in this.items)
            {
                if (item.value != null)
                {
                    item.value = DBValue.QuickFixHeight(item.value, heightbuf);
                }
            }
        }
        public void Pack(System.IO.Stream stream)
        {
            var extcount = this.extData == null ? 0 : extData.Count;
            if (extcount > 255)
                throw new Exception("too mush ExtData.");

            if (items.Count == 0 || items.Count > 65535)
            {
                throw new Exception("too mush items or no item.");
            }
            //pack extdata
            stream.WriteByte((byte)extcount);
            if (extcount > 0)
            {
                foreach (var item in extData)
                {
                    byte[] key = item.Key.ToBytes_UTF8Encode();
                    byte[] v = item.Value;
                    byte numkey = (byte)key.Length;
                    byte[] numv = BitConverter.GetBytes((UInt32)v.Length);
                    stream.WriteByte(numkey);
                    stream.Write(numv, 0, 4);
                    stream.Write(key, 0, numkey);
                    stream.Write(v, 0, v.Length);
                }
            }
            //pack item
            byte[] numitem = BitConverter.GetBytes((UInt16)items.Count);
            stream.Write(numitem, 0, 2);
            for (var i = 0; i < items.Count; i++)
            {
                items[i].Pack(stream);
            }
        }
        public static WriteTask UnPack(System.IO.Stream stream)
        {
            var task = new WriteTask();
            var extcount = stream.ReadByte();
            if (extcount > 0)
            {
                task.extData = new Dictionary<string, byte[]>();
            }
            for (var i = 0; i < extcount; i++)
            {
                var numkey = stream.ReadByte();
                byte[] bufnum = new byte[4];
                var numv = stream.Read(bufnum, 0, 4);
                UInt32 numValue = BitConverter.ToUInt32(bufnum, 0);
                byte[] bufv = new byte[Math.Max(numkey, numValue)];
                stream.Read(bufv, 0, numkey);
                var strkey = System.Text.Encoding.UTF8.GetString(bufv, 0, numkey);
                stream.Read(bufv, 0, (int)numValue);
                task.extData[strkey] = bufv;
            }
            byte[] bufnumitem = new byte[2];
            stream.Read(bufnumitem, 0, 2);
            var numitem = BitConverter.ToUInt16(bufnumitem, 0);
            for (var i = 0; i < numitem; i++)
            {
                task.items.Add(WriteTaskItem.UnPack(stream));
            }
            return task;
        }
        public byte[] ToBytes()
        {
            using (var ms = new System.IO.MemoryStream())
            {
                Pack(ms);
                return ms.ToArray();
            }
        }
        public static WriteTask FromRaw(byte[] data)
        {
            using (var ms = new System.IO.MemoryStream(data))
            {
                return UnPack(ms);
            }
        }
    }
}
