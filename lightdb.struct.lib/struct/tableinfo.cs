using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    public class TableInfo
    {
        public TableInfo(byte[] _tableid, string _tablename, string _tabledesc, DBValue.Type _keytype)
        {
            this.tableid = _tableid;
            this.tablename = _tablename;
            this.tabledesc = _tabledesc;
            this.keytype = _keytype;
        }
        public byte[] tableid;
        public string tablename;
        public string tabledesc;
        public DBValue.Type keytype;

        public void Pack(System.IO.Stream stream)
        {
            if (tableid.Length > 255)
                throw new Exception("tableid.Length>255");
            byte[] __tablename = tablename == null ? new byte[0] : System.Text.Encoding.UTF8.GetBytes(tablename);
            if (__tablename.Length > 255)
                throw new Exception("tablename.Length>255");
            byte[] __tabledesc = tabledesc == null ? new byte[0] : System.Text.Encoding.UTF8.GetBytes(tabledesc);
            if (__tabledesc.Length > 255)
                throw new Exception("tabledesc.Length>255");

            stream.WriteByte((byte)tableid.Length);
            stream.Write(tableid, 0, tableid.Length);
            stream.WriteByte((byte)__tablename.Length);
            stream.Write(__tablename, 0, __tablename.Length);
            stream.WriteByte((byte)__tabledesc.Length);
            stream.Write(__tabledesc, 0, __tabledesc.Length);
            stream.WriteByte((byte)keytype);
        }
        public static TableInfo UnPack(System.IO.Stream stream)
        {
            byte[] head = null;
            string name = null;
            string desc = null;
            DBValue.Type keytype = DBValue.Type.BigNumber;
            TableInfo table = new TableInfo(head, name, desc, keytype);
            byte[] buf = new byte[255];
            stream.Read(buf, 0, 1);
            table.tableid = new byte[buf[0]];
            stream.Read(table.tableid, 0, table.tableid.Length);
            stream.Read(buf, 0, 1);
            var strlen = buf[0];
            stream.Read(buf, 0, strlen);
            table.tablename = System.Text.Encoding.UTF8.GetString(buf, 0, strlen);
            stream.Read(buf, 0, 1);
            strlen = buf[0];
            stream.Read(buf, 0, strlen);
            table.tabledesc = System.Text.Encoding.UTF8.GetString(buf, 0, strlen);

            stream.Read(buf, 0, 1);
            table.keytype = (DBValue.Type)buf[0];

            return table;
        }
        public byte[] ToBytes()
        {
            using (var ms = new System.IO.MemoryStream())
            {
                Pack(ms);
                return ms.ToArray();
            }
        }
        public static TableInfo FromRaw(byte[] data)
        {
            using (var ms = new System.IO.MemoryStream(data))
            {
                return TableInfo.UnPack(ms);
            }
        }
    }
}
