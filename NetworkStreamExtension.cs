using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace Beanstalk.Core {

    public static class NetworkStreamExtension {

        public static string ReadResponse(this NetworkStream stream, Encoding encoding) {
            using (var ms = new MemoryStream()) {
                var last = -1;
                while (true) {
                    var ret = stream.ReadByte();
                    if (ret == -1) break;
                    ms.WriteByte((byte) ret);
                    if (last == 13 && ret == 10) break;
                    last = ret;
                }
                return encoding.GetString(ms.ToArray()).Trim();
            }
        }

    }

}