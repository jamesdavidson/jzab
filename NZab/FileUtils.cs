/**
 * Licensed to the zk1931 under one or more contributor license
 * agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;

namespace NZab
{
    /**
     * A static class that contains file-related utility methods.
     */
    internal static class FileUtils
    {
        /**
         * Atomically writes a long integer to a file.
         *
         * This method writes a long integer to a file by first writing the long
         * integer to a temporary file and then atomically moving it to the
         * destination, overwriting the destination file if it already exists.
         *
         * @param value a long integer value to write.
         * @param filePath path to the file to write the value to.
         */
        public static void WriteLongToFile(long value, string filePath)
        {
            string temp = Path.GetTempFileName();
            try
            {
                File.WriteAllText(temp, value.ToString(), Encoding.UTF8);
                AtomicMove(temp, filePath);
            }
            finally
            {
                if (File.Exists(temp))
                {
                    File.Delete(temp);
                }
            }
        }

        /**
         * Reads a long integer from a file.
         *
         * @param filePath file to read the integer value from.
         * @return the long integer value in the file
         */
        public static long ReadLongFromFile(string filePath)
        {
            string content = File.ReadAllText(filePath, Encoding.UTF8);
            return long.Parse(content.Trim());
        }

        /**
         * Atomically writes properties to a file.
         *
         * @param prop a NameValueCollection object to write.
         * @param filePath file to write the value to.
         */
        public static void WritePropertiesToFile(NameValueCollection prop, string filePath)
        {
            string temp = Path.GetTempFileName();
            try
            {
                using (StreamWriter sw = new StreamWriter(temp, false, Encoding.UTF8))
                {
                    foreach (string key in prop.AllKeys)
                    {
                        sw.WriteLine("{0}={1}", key, prop[key]);
                    }
                    sw.Flush();
                }
                AtomicMove(temp, filePath);
            }
            finally
            {
                if (File.Exists(temp))
                {
                    File.Delete(temp);
                }
            }
        }

        /**
         * Reads the Properties from a file.
         *
         * @param filePath file to read the Properties object from.
         * @return the NameValueCollection object in the file
         */
        public static NameValueCollection ReadPropertiesFromFile(string filePath)
        {
            NameValueCollection prop = new NameValueCollection();
            foreach (string line in File.ReadLines(filePath, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line) || line.StartsWith("#")) continue;
                int idx = line.IndexOf('=');
                if (idx > 0)
                {
                    string key = line.Substring(0, idx).Trim();
                    string val = line.Substring(idx + 1).Trim();
                    prop.Add(key, val);
                }
            }
            return prop;
        }

        /**
         * Atomically move one file to another file.
         *
         * @param source the source file.
         * @param dest the destination file.
         */
        public static void AtomicMove(string source, string dest)
        {
            if (File.Exists(dest))
            {
                File.Replace(source, dest, null);
            }
            else
            {
                File.Move(source, dest);
            }
        }
    }
}
