using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB
{
    public enum SplitWord : byte
    {
        TableInfo = 0x00,
        TableCount = 0x01,
        TableItem = 0x02,
    }
    public static class Helper
    {

        public static byte[] ToBytes_UTF8Encode(this string str)
        {
            return System.Text.Encoding.UTF8.GetBytes(str);
        }
        public static byte[] ToBytes_HexParse(this string str)
        {
            if (str.IndexOf("0x") == 0 || str.IndexOf("0X") == 0)
            {
                str = str.Substring(2);
            }
            byte[] data = new byte[str.Length / 2];
            for (var i = 0; i < str.Length / 2; i++)
            {
                data[i] = byte.Parse(str.Substring(i * 2, 2), System.Globalization.NumberStyles.HexNumber);
            }
            return data;
        }
        public static string ToString_Hex(this byte[] data)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var b in data)
            {
                sb.Append(b.ToString("x02"));
            }
            return sb.ToString();
        }
        public static bool BytesEquals(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;
            if (a1 == null || a2 == null || a1.Length != a2.Length)
                return false;
            unsafe
            {
                fixed (byte* p1 = a1, p2 = a2)
                {
                    byte* x1 = p1, x2 = p2;
                    int l = a1.Length;
                    for (int i = 0; i < l / 8; i++, x1 += 8, x2 += 8)
                        if (*((long*)x1) != *((long*)x2)) return false;
                    if ((l & 4) != 0) { if (*((int*)x1) != *((int*)x2)) return false; x1 += 4; x2 += 4; }
                    if ((l & 2) != 0) { if (*((short*)x1) != *((short*)x2)) return false; x1 += 2; x2 += 2; }
                    if ((l & 1) != 0) if (*((byte*)x1) != *((byte*)x2)) return false;
                    return true;
                }
            }
        }
        public static string ToString_UTF8Decode(this byte[] data)
        {
            return System.Text.Encoding.UTF8.GetString(data);
        }

        public static UInt64 ToUInt64(this byte[] data)
        {
            return BitConverter.ToUInt64(data, 0);
        }
        public static byte[] ToBytes(this UInt64 value)
        {
            return BitConverter.GetBytes(value);
        }

        public static byte[] CalcKey(byte[] head, byte[] key, SplitWord splitWord = SplitWord.TableItem)
        {
            if (head.Length > 255)
                throw new Exception("not support key >255 bytes");
            byte[] finalkey = new byte[1 + head.Length + 1 + (key != null ? key.Length : 0)];
            //key的构成 keylen + key + splitword + value
            //splitword 00 controlitem
            //splitword 01 valueitem
            //splitword >0 otherinfo
            finalkey[0] = (byte)head.Length;
            for (var i = 0; i < head.Length; i++)
            {
                finalkey[i + 1] = head[i];
            };
            finalkey[1 + head.Length] = (byte)splitWord;
            if (key != null)
            {
                for (var i = 0; i < key.Length; i++)
                {
                    finalkey[1 + head.Length + 1 + i] = key[i];
                }
            }
            return finalkey;
        }

    }
}
