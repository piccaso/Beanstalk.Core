using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Beanstalk.Core {

    public class BeanstalkConnection : IDisposable {

        private static readonly TimeSpan DefaultDelay = TimeSpan.Zero;

        private static readonly TimeSpan DefaultTtr = TimeSpan.FromMinutes(1);

        private static readonly uint DefaultPriority = 1024;

        private readonly string _host;

        private readonly ushort _port;

        private readonly Encoding _encoding;

        private TcpClient _client;

        public BeanstalkConnection(string host, ushort port, Encoding encoding = null) {
            _host = host;
            _port = port;
            _encoding = encoding ?? BeanstalkFactory.DefaultEncoding;
        }

        private async Task<NetworkStream> GetStream() {
            if (_client != null && _client.Connected) return _client.GetStream();
            _client = new TcpClient();
            await _client.ConnectAsync(_host, _port);
            return _client.GetStream();
        }

        public void Dispose() { _client.Dispose(); }

        public async Task<ulong> Put(string data, TimeSpan delay, uint priority, TimeSpan ttr) {
            return await new Command(await GetStream(), _encoding)
                .Put(priority, (uint) delay.TotalSeconds, (uint) ttr.TotalSeconds, data);
        }

        public async Task<ulong> Put(string data) {
            return await new Command(await GetStream(), _encoding)
                .Put(DefaultPriority, (uint) DefaultDelay.TotalSeconds, (uint) DefaultTtr.TotalSeconds, data);
        }

        public async Task<ulong> Put(string data, uint priority) {
            return await new Command(await GetStream(), _encoding)
                .Put(priority, (uint) DefaultDelay.TotalSeconds, (uint) DefaultTtr.TotalSeconds, data);
        }

        public async Task<ulong> Put(string data, TimeSpan delay) {
            return await new Command(await GetStream(), _encoding)
                .Put(DefaultPriority, (uint) delay.TotalSeconds, (uint) DefaultTtr.TotalSeconds, data);
        }

        public async Task Use(string tube) { await new Command(await GetStream(), _encoding).Use(tube); }

        public async Task<IJob> Reserve() {
            return await new Command(await GetStream(), _encoding).Reserve((uint) DefaultTtr.TotalSeconds);
        }

        public async Task<IJob> Reserve(TimeSpan timeout) {
            return await new Command(await GetStream(), _encoding).Reserve((uint) timeout.TotalSeconds);
        }

        public async Task Delete(ulong id) { await new Command(await GetStream(), _encoding).Delete(id); }

        public async Task Release(ulong id) {
            await new Command(await GetStream(), _encoding).Release(id, DefaultPriority, (uint) DefaultDelay.TotalSeconds);
        }

        public async Task Release(ulong id, uint priority) {
            await new Command(await GetStream(), _encoding).Release(id, priority, (uint) DefaultDelay.TotalSeconds);
        }

        public async Task Release(ulong id, TimeSpan delay) {
            await new Command(await GetStream(), _encoding).Release(id, DefaultPriority, (uint) delay.TotalSeconds);
        }

        public async Task Release(ulong id, uint priority, TimeSpan delay) {
            await new Command(await GetStream(), _encoding).Release(id, priority, (uint) delay.TotalSeconds);
        }

        public async Task Bury(ulong id) { await new Command(await GetStream(), _encoding).Bury(id, DefaultPriority); }

        public async Task Bury(ulong id, uint priority) { await new Command(await GetStream(), _encoding).Bury(id, priority); }

        public async Task Touch(ulong id) { await new Command(await GetStream(), _encoding).Touch(id); }

        public async Task Watch(string tube) { await new Command(await GetStream(), _encoding).Watch(tube); }

        public async Task Ignore(string tube) { await new Command(await GetStream(), _encoding).Ignore(tube); }

        public async Task<IJob> Peek(ulong id) { return await new Command(await GetStream(), _encoding).Peek(id); }

        public async Task<IJob> PeekReady() { return await new Command(await GetStream(), _encoding).PeekReady(); }

        public async Task<IJob> PeekDelayed() { return await new Command(await GetStream(), _encoding).PeekDelayed(); }

        public async Task<IJob> PeekBuried() { return await new Command(await GetStream(), _encoding).PeekDelayed(); }

        public async Task<uint> Kick(uint bound) { return await new Command(await GetStream(), _encoding).Kick(bound); }

        public async Task KickJob(ulong id) { await new Command(await GetStream(), _encoding).KickJob(id); }

        public async Task<string> StatsJob(ulong id) { return await new Command(await GetStream(), _encoding).StatsJob(id); }

        public async Task<string> StatsTube(string tube) {
            return await new Command(await GetStream(), _encoding).StatsTube(tube);
        }

        public async Task<string> Stats() { return await new Command(await GetStream(), _encoding).Stats(); }

        public async Task<string> ListTubes() { return await new Command(await GetStream(), _encoding).ListTubes(); }

        public async Task<string> ListTubeUsed() { return await new Command(await GetStream(), _encoding).ListTubeUsed(); }

        public async Task<string> ListTubesWatched() { return await new Command(await GetStream(), _encoding).ListTubesWatched(); }

        public async Task Pause(string tube) {
            await new Command(await GetStream(), _encoding).PauseTube(tube, (uint) DefaultDelay.TotalSeconds);
        }

        public async Task Pause(string tube, TimeSpan delay) {
            await new Command(await GetStream(), _encoding).PauseTube(tube, (uint) delay.TotalSeconds);
        }

        public async Task Quit() { await new Command(await GetStream(), _encoding).Quit(); }

    }

}