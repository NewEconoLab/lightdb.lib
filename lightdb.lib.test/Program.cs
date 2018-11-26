using System;
using System.Collections.Generic;

namespace LightDB.test
{
    class Program
    {
        static void InitMenu()
        {
            //把当前目录搞对，怎么启动都能找到dll了
            var lastpath = System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location); ;
            Console.WriteLine("lastpath=" + lastpath);
            Environment.CurrentDirectory = lastpath;

            test_db_open(null);

            AddMenu("exit", "exit application", (words) => { Environment.Exit(0); });
            AddMenu("help", "show help", ShowMenu);
            AddMenu("test.db.open", "open/create a db on a path", test_db_open);
            AddMenu("test.db.close", "close db", test_db_close);
            AddMenu("test.db.tablecreate", "create a table", test_db_tablecreate);
            AddMenu("test.db.tabledelete", "delete a table", test_db_tabledelete);
            AddMenu("test.db.tablewrite", "write a table", test_db_tablewrite);
            AddMenu("test.db.tableinfo", "get a table info", test_db_tableinfo);
            AddMenu("test.db.tableserach", "serach a table", test_db_tableserach);
            AddMenu("test.db.enumblock", "enum every writeblock", test_db_enumblock);
            AddMenu("test.db.checkpoint", "make a checkpoint db", test_db_checkpoint);
        }
        static LightDB db = null;
        static void test_db_open(string[] words)
        {
            try
            {
                Console.WriteLine("open db");
                db = new LightDB();
                //打开一个数据库，打开时如果不存在会创建一个
                db.Open("../db001");
            }
            catch
            {
                //try
                {
                    Console.WriteLine("create db");
                    db.Open("../db001", new DBCreateOption() { MagicStr = "hello world." });
                }
                //catch (Exception err)
                //{
                //    Console.WriteLine("error:" + err.Message);
                //}
            }
        }
        static void test_db_close(string[] words)
        {
            try
            {
                Console.WriteLine("close db");
                db.Close();
                db = null;
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_tablecreate(string[] words)
        {
            try
            {

                Console.WriteLine("test db table");

                using (var snap = db.UseSnapShot())
                {
                    var writetask = db.CreateWriteTask();
                    {
                        var info = new TableInfo(
                            new byte[] { 0x01, 0x02, 0x03 },//tableid 是区分表格的数据，至少长度2，太短的不允许
                                                            //下面三个参数都是提供表的信息，无所谓什么
                            "mytable",//tablename 
                            "testtable0001",//tabledesc
                            DBValue.Type.String//tablekeytype
                            );
                        writetask.CreateTable(info);

                        for (var i = 0; i < 100; i++)
                        {
                            var key = ("key" + i).ToBytes_UTF8Encode();
                            writetask.Put(new byte[] { 0x01, 0x02, 0x03 }, key, DBValue.FromValue(DBValue.Type.UINT32, (UInt32)i));

                        }
                        db.Write(writetask);
                    }
                }
                Console.WriteLine("create table and write 100 item.");
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_tablewrite(string[] words)
        {
            try
            {

                Console.WriteLine("test db table");

                using (var snap = db.UseSnapShot())
                {
                    var writetask = db.CreateWriteTask();
                    {
                        //写100个uint32
                        for (var i = 0; i < 100; i++)
                        {
                            var key = ("key" + i).ToBytes_UTF8Encode();
                            writetask.Put(new byte[] { 0x01, 0x02, 0x03 }, key, DBValue.FromValue(DBValue.Type.UINT32, (UInt32)i));
                        }
                        //写100个字符串
                        for (var i = 0; i < 100; i++)
                        {
                            var key = ("skey" + i).ToBytes_UTF8Encode();
                            writetask.Put(new byte[] { 0x01, 0x02, 0x03 }, key, DBValue.FromValue(DBValue.Type.String, "abcdefg" + i));
                        }

                        db.Write(writetask);
                    }

                }
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_tableinfo(string[] words)
        {
            try
            {
                Console.WriteLine("test db table");
                using (var snap = db.UseSnapShot())
                {
                    var info = snap.GetTableInfo(new byte[] { 0x01, 0x02, 0x03 });
                    var count = snap.GetTableCount(new byte[] { 0x01, 0x02, 0x03 });
                    Console.WriteLine("get table info: name=" + info.tablename);
                    Console.WriteLine("get table count =" + count);
                    Console.WriteLine("snapheight=" + snap.DataHeight);

                }
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_tabledelete(string[] words)
        {
            try
            {
                Console.WriteLine("test db table");
                using (var snap = db.UseSnapShot())
                {
                    var writetask = db.CreateWriteTask();
                    {
                        writetask.DeleteTable(new byte[] { 0x01, 0x02, 0x03 });
                        db.Write(writetask);
                    }
                }
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_tableserach(string[] words)
        {
            try
            {
                Console.WriteLine("test db table");
                using (var snap = db.UseSnapShot())
                {
                    var keyfinder = snap.CreateKeyFinder(new byte[] { 0x01, 0x02, 0x03 });
                    foreach (byte[] key in keyfinder)
                    {
                        var strkey = System.Text.Encoding.UTF8.GetString(key);
                        Console.WriteLine("got a key:" + strkey);
                    }
                }
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        static void test_db_checkpoint(string[] words)
        {
            //建立当前数据库的副本
            db.CheckPoint("d:\\db_cp001");
            //根据rocksdb的说明，这个建立副本贼鸡儿高效

            //然后就从这个数据库读了，这是作为历史记录的数据库，对他只读，别去瞎几把改
            var db2 = new LightDB();
            db2.OpenRead("d:\\db_cp001");

            var snap = db2.UseSnapShot();

            var table = snap.GetTableInfo(new byte[] { 01, 02, 03 });
            Console.WriteLine("db2 table=" + table.tablename);

            Console.WriteLine("db2 dataheight=" + snap.DataHeight);
        }
        static void test_db_enumblock(string[] words)
        {
            try
            {
                Console.WriteLine("test_db_enumblock");
                using (var snap = db.UseSnapShot())
                {
                    Console.WriteLine("now snap height=" + snap.DataHeight);
                    var keyfinder = snap.CreateKeyFinder(LightDB.systemtable_block);
                    foreach (byte[] key in keyfinder)
                    {
                        var longkey = BitConverter.ToUInt64(key);
                        Console.WriteLine("got a key:" + longkey);
                        var data = snap.GetValue(LightDB.systemtable_block, key);
                        var task = WriteTask.FromRaw(data.value);
                        Console.WriteLine("   task count=" + task.items.Count);
                        foreach (var item in task.items)
                        {
                            Console.WriteLine("   item:" + item.op + ":" + item.key.Length + "," + item.value.Length);
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Console.WriteLine("error:" + err.Message);
            }
        }
        public static System.Collections.Generic.Dictionary<string, Action<string[]>> menuItem = new System.Collections.Generic.Dictionary<string, Action<string[]>>();
        public static System.Collections.Generic.Dictionary<string, string> menuDesc = new System.Collections.Generic.Dictionary<string, string>();
        static void AddMenu(string cmd, string desc, Action<string[]> onMenu)
        {
            menuItem[cmd.ToLower()] = onMenu;
            menuDesc[cmd.ToLower()] = desc;
        }

        static void ShowMenu(string[] words = null)
        {
            Console.WriteLine("==Menu==");
            foreach (var key in menuItem.Keys)
            {
                var line = "  " + key + " - ";
                if (menuDesc.ContainsKey(key))
                    line += menuDesc[key];
                Console.WriteLine(line);
            }
        }
        static void MenuLoop()
        {
            while (true)
            {
                Console.Write("-->");
                var line = Console.ReadLine();
                var words = line.Split(" ", StringSplitOptions.RemoveEmptyEntries);
                if (words.Length > 0)
                {
                    var cmd = words[0].ToLower();
                    if (cmd == "?")
                    {
                        ShowMenu();
                    }
                    else if (menuItem.ContainsKey(cmd))
                    {
                        menuItem[cmd](words);
                    }
                }
            }
        }
        static void Main(string[] args)
        {
            Console.WriteLine("lightchain.db test.");
            InitMenu();
            MenuLoop();
        }
    }
}
