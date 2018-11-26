using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    public class DBValue
    {
        public enum Type : byte
        {
            Deleted,//如果value是这个值，表示该值已经被删除了
            Bytes,
            INT32,
            UINT32,
            INT64,
            UINT64,
            BOOL,
            Float32,
            Float64,
            BigNumber,
            String,
            //Struct
            //数组
            //字典 这类复杂数据结构不提供
        }
        public Type type;
        public byte[] tag;
        public ulong lastHeight;//最后修改高度
        public byte[] value;
        public object typedvalue;
        private DBValue()
        {

        }
        public static bool BytesEqualWithoutHeight(byte[] a, byte[] b)
        {
            if (ReferenceEquals(a, b))
                return true;
            if (a == null && b == null)
                return true;
            if (a.Length != b.Length)
                return false;
            if (a == null || b == null)
                return false;

            var tagLength = a[1];
            for (var i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                    return false;
                if (i >= tagLength + 2 && i < tagLength + 2 + 8)
                {
                    continue;
                }
            }
            return true;
        }
        public static byte[] QuickFixHeight(byte[] data, byte[] heightbuf)
        {
            int seek = 0;

            //type
            //var v = data[0];
            seek++;

            //tag
            var tagLength = data[seek]; seek++;
            seek += tagLength;

            //data
            var datalen = BitConverter.ToUInt32(data, seek); seek += 4;
            seek += (int)datalen;


            //timestep
            if (seek == data.Length)//add 8 byte
            {
                byte[] newdata = new byte[data.Length + 8];
                for (var i = 0; i < data.Length; i++)
                {
                    newdata[i] = data[i];
                }
                data = newdata;
            }
            for (var i = 0; i < 8; i++)
            {
                data[seek + i] = heightbuf[i];
            }

            return data;
        }
        public static byte[] QuickGetHeight(byte[] data)
        {
            byte[] heightbuf = new byte[8];
            var tagLength = data[1];
            for (var i = 0; i < 8; i++)
            {
                heightbuf[i] = data[tagLength + 2 + i];
            }
            return heightbuf;
        }
        public static DBValue DeletedValue
        {
            get
            {
                DBValue v = new DBValue();
                v.type = Type.Deleted;
                v.tag = new byte[0];
                v.value = new byte[0];
                v.typedvalue = null;
                v.lastHeight = 0;
                return v;
            }
        }
        public static DBValue FromValue(Type _type, object _value)
        {
            DBValue v = new DBValue();
            v.type = _type;
            v.tag = new byte[0];
            if (v.type == Type.Bytes && _value is byte[])
            {
                v.value = (byte[])_value;
            }
            else if (v.type == Type.INT32 && _value is Int32)
            {
                v.value = BitConverter.GetBytes((Int32)_value);
            }
            else if (v.type == Type.UINT32 && _value is UInt32)
            {
                v.value = BitConverter.GetBytes((UInt32)_value);

            }
            else if (v.type == Type.INT64 && _value is Int64)
            {
                v.value = BitConverter.GetBytes((Int64)_value);

            }
            else if (v.type == Type.UINT64 && _value is UInt64)
            {
                v.value = BitConverter.GetBytes((UInt64)_value);

            }
            else if (v.type == Type.BOOL && _value is bool)
            {
                v.value = ((bool)_value) ? new byte[1] { 1 } : new byte[] { 0 };
            }
            else if (v.type == Type.Float32 && _value is float)
            {
                v.value = BitConverter.GetBytes((float)_value);

            }
            else if (v.type == Type.Float64 && _value is double)
            {
                v.value = BitConverter.GetBytes((double)_value);

            }
            else if (v.type == Type.BigNumber && _value is System.Numerics.BigInteger)
            {
                v.value = ((System.Numerics.BigInteger)_value).ToByteArray();
            }
            else if (v.type == Type.String && _value is string)
            {
                v.value = System.Text.Encoding.UTF8.GetBytes((string)_value);
            }
            else
            {
                throw new Exception("error value type:want[" + _type + "] value is[" + _value.GetType() + "]");
            }
            v.typedvalue = _value;
            return v;
        }
        public static DBValue FromRaw(byte[] data)
        {
            if (data == null || data.Length == 0)
                return null;
            DBValue v = new DBValue();
            int seek = 0;
            //read type
            v.type = (Type)data[seek]; seek++;
            //read tag
            v.tag = new byte[data[seek]]; seek++;
            for (var i = 0; i < v.tag.Length; i++)
            {
                v.tag[i] = data[seek]; seek++;
            }
            //read value
            UInt32 datalen = BitConverter.ToUInt32(data, seek); seek += 4;
            v.value = new byte[datalen];
            for (var i = 0; i < v.value.Length; i++)
            {
                v.value[i] = data[seek]; seek++;
            }
            if (seek < data.Length)
            {
                //read last
                v.lastHeight = BitConverter.ToUInt64(data, seek); seek += 8;
            }
            v.ParseValue();
            return v;
        }
        public byte[] ToBytes(bool withLastModifyHeight)
        {
            byte[] data = new byte[2 + this.tag.Length + 4 + this.value.Length + (withLastModifyHeight ? 8 : 0)];
            int seek = 0;
            //write type
            data[seek] = (byte)this.type; seek++;
            //write tag
            data[seek] = (byte)this.tag.Length; seek++;
            for (var i = 0; i < this.tag.Length; i++)
            {
                data[seek] = this.tag[i]; seek++;
            }
            //write value;
            var datalen = BitConverter.GetBytes((UInt32)this.value.Length);
            for (var i = 0; i < 4; i++)
            {
                data[seek] = datalen[i]; seek++;
            }
            for (var i = 0; i < this.value.Length; i++)
            {
                data[seek] = this.value[i]; seek++;
            }
            //write last
            if (withLastModifyHeight)
            {
                byte[] last = BitConverter.GetBytes(this.lastHeight);
                for (var i = 0; i < 8; i++)
                {
                    data[seek] = last[i]; seek++;
                }
            }
            return data;
        }

        public void ParseValue()
        {
            switch (this.type)
            {
                case Type.Deleted:
                    break;
                case Type.Bytes:
                    break;
                case Type.INT32:
                    typedvalue = BitConverter.ToInt32(this.value, 0);
                    break;
                case Type.UINT32:
                    typedvalue = BitConverter.ToUInt32(this.value, 0);
                    break;
                case Type.INT64:
                    typedvalue = BitConverter.ToInt64(this.value, 0);
                    break;
                case Type.UINT64:
                    typedvalue = BitConverter.ToUInt64(this.value, 0);
                    break;
                case Type.BOOL:
                    typedvalue = (this.value[0] > 0);
                    break;
                case Type.Float32:
                    typedvalue = BitConverter.ToSingle(this.value, 0);
                    break;
                case Type.Float64:
                    typedvalue = BitConverter.ToDouble(this.value, 0);
                    break;
                case Type.BigNumber:
                    typedvalue = new System.Numerics.BigInteger(this.value);
                    break;
                case Type.String:
                    typedvalue = System.Text.Encoding.UTF8.GetString(this.value);
                    break;
            }


        }
        public int AsInt32()
        {
            if (this.type == Type.INT32)
            {
                return (int)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (int)(uint)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (int)(ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (int)(long)typedvalue;
            }
            else if (this.type == Type.Float32)
            {
                return (int)(float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (int)(double)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (int)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public uint AsUInt32()
        {
            if (this.type == Type.UINT32)
            {
                return (uint)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (uint)(int)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (uint)(ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (uint)(long)typedvalue;
            }
            else if (this.type == Type.Float32)
            {
                return (uint)(float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (uint)(double)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (uint)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public long AsInt64()
        {
            if (this.type == Type.INT64)
            {
                return (long)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (long)(ulong)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (long)(uint)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (long)(int)typedvalue;
            }
            else if (this.type == Type.Float32)
            {
                return (long)(float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (long)(double)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (long)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public ulong AsUInt64()
        {
            if (this.type == Type.UINT64)
            {
                return (ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (ulong)(long)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (ulong)(uint)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (ulong)(int)typedvalue;
            }
            else if (this.type == Type.Float32)
            {
                return (ulong)(float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (ulong)(double)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (ulong)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public System.Numerics.BigInteger AsBigInteger()
        {
            if (this.type == Type.BigNumber)
            {
                return (System.Numerics.BigInteger)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (long)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (uint)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (int)typedvalue;
            }
            else if (this.type == Type.Float32)
            {
                return (long)(float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (long)(double)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public float AsFloat32()
        {
            if (this.type == Type.Float32)
            {
                return (float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (float)(double)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (float)(int)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (float)(uint)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (float)(ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (float)(long)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (float)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public double AsFloat64()
        {
            if (this.type == Type.Float32)
            {
                return (float)typedvalue;
            }
            else if (this.type == Type.Float64)
            {
                return (double)typedvalue;
            }
            else if (this.type == Type.INT32)
            {
                return (double)(int)typedvalue;
            }
            else if (this.type == Type.UINT32)
            {
                return (double)(uint)typedvalue;
            }
            else if (this.type == Type.UINT64)
            {
                return (double)(ulong)typedvalue;
            }
            else if (this.type == Type.INT64)
            {
                return (double)(long)typedvalue;
            }
            else if (this.type == Type.BigNumber)
            {
                return (double)(System.Numerics.BigInteger)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public string AsString()
        {
            if (this.type == Type.String)
            {
                return (string)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
        public bool AsBool()
        {
            if (this.type == Type.BOOL)
            {
                return (bool)typedvalue;
            }
            else
            {
                throw new Exception("do not known how to convert it.");
            }
        }
    }

}
