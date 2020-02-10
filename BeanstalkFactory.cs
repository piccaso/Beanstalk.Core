using System.Text;

namespace Beanstalk.Core {

    public class BeanstalkFactory {

        internal static Encoding DefaultEncoding = Encoding.ASCII;

        private readonly string _host;

        private readonly ushort _port;

        private readonly Encoding _encoding;

        public BeanstalkFactory(string host, ushort port, Encoding encoding = null) {
            _host = host;
            _port = port;
            _encoding = encoding ?? DefaultEncoding;
        }

        public BeanstalkConnection GetConnection() => new BeanstalkConnection(_host, _port, _encoding);
    }

}