using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Beanstalk.Core {

    public class Command : IConnection {

        private readonly NetworkStream _stream;
        private readonly Encoding _encoding;
        private byte[] _command;

        private readonly Dictionary<string, Action<string>> _checkers = new Dictionary<string, Action<string>>();

        public Command(NetworkStream stream, Encoding encoding) {
            _stream = stream;
            _encoding = encoding;
            _checkers.Add("INTERNAL_ERROR", resp => throw new BeanstalkException(resp));
            _checkers.Add("OUT_OF_MEMORY", resp => throw new BeanstalkException(resp));
            _checkers.Add("BAD_FORMAT", resp => throw new BeanstalkException(resp));
            _checkers.Add("UNKNOWN_COMMAND", resp => throw new BeanstalkException(resp));
        }

        private async Task<string[]> Expect(string code) {
            await _stream.WriteAsync(_command, 0, _command.Length);
            var response = _stream.ReadResponse(_encoding);
            var split = response.Split(' ');
            _checkers.FirstOrDefault(item => item.Key == split[0]).Value?.Invoke(response);
            if (code != split[0]) throw new BeanstalkException($"FATAL_ERROR: expect {code}, {split[0]} gave.");
            return split;
        }

        public async Task<ulong> Put(uint priority, uint delay, uint ttr, string data) {
            var len = _encoding.GetByteCount(data);
            Console.WriteLine("Data {0} Length: {1}", data, len);
            _command = _encoding.GetBytes($"put {priority} {delay} {ttr} {len}\r\n{data}\r\n");
            _checkers.Add("BURIED", resp => throw new BeanstalkException(resp));
            _checkers.Add("EXPECTED_CRLF", resp => throw new BeanstalkException(resp));
            _checkers.Add("JOB_TOO_BIG", resp => throw new BeanstalkException(resp));
            _checkers.Add("DRAINING", resp => throw new BeanstalkException(resp));
            var response = await Expect("INSERTED");
            return ulong.Parse(response[1]);
        }

        public async Task Use(string tube) {
            _command = _encoding.GetBytes($"use {tube}\r\n");
            await Expect("USING");
        }

        public async Task<IJob> Reserve(uint timeout) {
            _command = _encoding.GetBytes($"reserve-with-timeout {timeout}\r\n");
            _checkers.Add("DEADLINE_SOON", resp => throw new BeanstalkException(resp));
            _checkers.Add("TIMED_OUT", resp => throw new BeanstalkException(resp));
            return await ParseJob(await Expect("RESERVED"));
        }

        public async Task Delete(ulong id) {
            _command = _encoding.GetBytes($"delete {id}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("DELETED");
        }

        public async Task Release(ulong id, uint priority, uint delay) {
            _command = _encoding.GetBytes($"release {id} {priority} {delay}\r\n");
            _checkers.Add("BURIED", resp => throw new BeanstalkException(resp));
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("RELEASED");
        }

        public async Task Bury(ulong id, uint priority) {
            _command = _encoding.GetBytes($"bury {id} {priority}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("BURIED");
        }

        public async Task Touch(ulong id) {
            _command = _encoding.GetBytes($"touch {id}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("TOUCHED");
        }

        public async Task<uint> Watch(string tube) {
            _command = _encoding.GetBytes($"watch {tube}\r\n");
            var response = await Expect("WATCHING");
            return uint.Parse(response[1]);
        }

        public async Task<uint> Ignore(string tube) {
            _command = _encoding.GetBytes($"ignore {tube}\r\n");
            _checkers.Add("NOT_IGNORED", resp => throw new BeanstalkException(resp));
            var response = await Expect("WATCHING");
            return uint.Parse(response[1]);
        }

        public async Task<IJob> Peek(ulong id) {
            _command = _encoding.GetBytes($"peek {id}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseJob(await Expect("FOUND"));
        }

        public async Task<IJob> PeekReady() {
            _command = _encoding.GetBytes("peek-ready\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseJob(await Expect("FOUND"));
        }

        public async Task<IJob> PeekDelayed() {
            _command = _encoding.GetBytes("peek-delayed\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseJob(await Expect("FOUND"));
        }

        public async Task<IJob> PeekBuried() {
            _command = _encoding.GetBytes("peek-buried\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseJob(await Expect("FOUND"));
        }

        public async Task<uint> Kick(uint bound) {
            _command = _encoding.GetBytes($"kick {bound}\r\n");
            var response = await Expect("KICKED");
            return uint.Parse(response[1]);
        }

        public async Task KickJob(ulong id) {
            _command = _encoding.GetBytes($"kick-job {id}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("KICKED");
        }

        public async Task<string> StatsJob(ulong id) {
            _command = _encoding.GetBytes($"stats-job {id}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseOk(await Expect("OK"));
        }

        public async Task<string> StatsTube(string tube) {
            _command = _encoding.GetBytes($"stats-tube {tube}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            return await ParseOk(await Expect("OK"));
        }

        public async Task<string> Stats() {
            _command = _encoding.GetBytes("stats\r\n");
            return await ParseOk(await Expect("OK"));
        }

        public async Task<string> ListTubes() {
            _command = _encoding.GetBytes("list-tubes\r\n");
            return await ParseOk(await Expect("OK"));
        }

        public async Task<string> ListTubeUsed() {
            _command = _encoding.GetBytes("list-tube-used\r\n");
            var response = await Expect("USING");
            return response[1];
        }

        public async Task<string> ListTubesWatched() {
            _command = _encoding.GetBytes("list-tubes-watched\r\n");
            return await ParseOk(await Expect("OK"));
        }

        public async Task PauseTube(string tube, uint delay) {
            _command = _encoding.GetBytes($"pause-tube {tube} {delay}\r\n");
            _checkers.Add("NOT_FOUND", resp => throw new BeanstalkException(resp));
            await Expect("PAUSED");
        }

        public async Task Quit() {
            _command = _encoding.GetBytes("quit\r\n");
            await Expect(string.Empty);
        }

        private async Task<Job> ParseJob(IReadOnlyList<string> response) {
            var len = int.Parse(response[2]) + 2;
            var bytes = new byte[len];
            await _stream.ReadAsync(bytes, 0, len);
            return new Job {
                Id = ulong.Parse(response[1]),
                Data = _encoding.GetString(bytes)
            };
        }

        private async Task<string> ParseOk(IReadOnlyList<string> response) {
            var len = int.Parse(response[1]) + 2;
            var bytes = new byte[len];
            await _stream.ReadAsync(bytes, 0, len);
            return _encoding.GetString(bytes);
        }

    }

}