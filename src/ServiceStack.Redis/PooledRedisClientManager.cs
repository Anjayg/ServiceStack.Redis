//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2010 Liquidbit Ltd.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ServiceStack.Common.Web;
using ServiceStack.Logging;

namespace ServiceStack.Redis
{
	/// <summary>
	/// Provides thread-safe pooling of redis client connections.
	/// Allows load-balancing of master-write and read-slave hosts, ideal for
	/// 1 master and multiple replicated read slaves.
	/// </summary>
	public partial class PooledRedisClientManager
		: IRedisClientsManager
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(PooledRedisClientManager));

        /// <summary>
        /// Ĭ�ϵ����ӳ����� 10
        /// </summary>
		protected const int PoolSizeMultiplier = 10;
        
        /// <summary>
        /// �ɶ�д������IP��Ϣ
        /// </summary>
		private List<EndPoint> ReadWriteHosts { get; set; }
        /// <summary>
        /// ֻ��������IP��Ϣ
        /// </summary>
		private List<EndPoint> ReadOnlyHosts { get; set; }

        /// <summary>
        /// ��д���б�
        /// </summary>
		private RedisClient[] writeClients = new RedisClient[0];
        /// <summary>
        /// ��д�ص�ǰָ��
        /// </summary>
		protected int WritePoolIndex;

        /// <summary>
        /// ֻ�����б�
        /// </summary>
		private RedisClient[] readClients = new RedisClient[0];
        /// <summary>
        /// ֻ����ָ��
        /// </summary>
		protected int ReadPoolIndex;
        /// <summary>
        /// �ͻ�������
        /// </summary>
		protected int RedisClientCounter = 0;

        /// <summary>
        /// ������Ϣ
        /// </summary>
		protected RedisClientManagerConfig Config { get; set; }

        /// <summary>
        /// �ͻ��˹���
        /// </summary>
		public IRedisClientFactory RedisClientFactory { get; set; }
        /// <summary>
        /// ѡ���DB���
        /// </summary>
		public int Db { get; private set; }

        public int Count(Func<RedisClient, bool> predicate = null)
        {
            if (predicate == null)
            {
                return this.writeClients.Length;
            }
            else
            {
                return this.writeClients.Count(predicate);
            }
        }

        #region - ���� -
        public PooledRedisClientManager() : this(RedisNativeClient.DefaultHost) { }

        public PooledRedisClientManager(params string[] readWriteHosts)
            : this(readWriteHosts, readWriteHosts)
        {
        }

        public PooledRedisClientManager(IEnumerable<string> readWriteHosts, IEnumerable<string> readOnlyHosts)
            : this(readWriteHosts, readOnlyHosts, null)
        {
        }

        /// <summary>
        /// Hosts can be an IP Address or Hostname in the format: host[:port]
        /// e.g. 127.0.0.1:6379
        /// default is: localhost:6379
        /// </summary>
        /// <param name="readWriteHosts">The write hosts.</param>
        /// <param name="readOnlyHosts">The read hosts.</param>
        /// <param name="config">The config.</param>
        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config)
            : this(readWriteHosts, readOnlyHosts, config, RedisNativeClient.DefaultDb)
        {
        }

        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            int initalDb)
            : this(readWriteHosts, readOnlyHosts, null, initalDb)
        {
        }

        public PooledRedisClientManager(
            IEnumerable<string> readWriteHosts,
            IEnumerable<string> readOnlyHosts,
            RedisClientManagerConfig config,
            int initalDb)
        {
            //û�����õľ��ǵ�һ��DB
            this.Db = config != null
                ? config.DefaultDb.GetValueOrDefault(initalDb)
                : initalDb;

            //��127.0.0.1:6379ת��һ��EndPoint����
            ReadWriteHosts = readWriteHosts.ToIpEndPoints();
            ReadOnlyHosts = readOnlyHosts.ToIpEndPoints();

            this.RedisClientFactory = Redis.RedisClientFactory.Instance;

            this.Config = config ?? new RedisClientManagerConfig
            {
                MaxWritePoolSize = ReadWriteHosts.Count * PoolSizeMultiplier,
                MaxReadPoolSize = ReadOnlyHosts.Count * PoolSizeMultiplier,
            };

            if (this.Config.AutoStart)
            {
                this.OnStart();
            }
        } 
        #endregion

		protected virtual void OnStart()
		{
			this.Start();
		}

		/// <summary>
		/// Returns a Read/Write client (The default) using the hosts defined in ReadWriteHosts
		/// </summary>
		/// <returns></returns>
        public IRedisClient GetClient()
        {
            return this.GetClient(-1);
        }

        public IRedisClient GetClient(int index)
		{
            //������д��
			lock (writeClients)
			{
                //�����ж�
				AssertValidReadWritePool();

				RedisClient inActiveClient;
                //�ҵ�һ�����еĿͻ���
                while ((inActiveClient = GetInActiveWriteClient(index)) == null)
				{
                    //�ͷŶ�д�أ��Է�ֹ���������߳�
					Monitor.Wait(writeClients);
				}

				WritePoolIndex++;
				inActiveClient.Active = true;//����������пͻ���

				//Reset database to default if changed
                //�л�����ȷ��DB��
				if (inActiveClient.Db != Db)
				{
					inActiveClient.Db = Db;
				}

				return inActiveClient;
			}
		}

		/// <summary>
		/// Called within a lock
		/// </summary>
		/// <returns></returns>
		private RedisClient GetInActiveWriteClient(int index)
		{
            var start = 0;
            var step = 1;
            if (index > -1 && index < ReadWriteHosts.Count)
            {
                start = index;
                step = ReadWriteHosts.Count;
            }
            //������д��
            //���ʱ�����������
            for (var i = start; i < writeClients.Length; i += step)
			{
                //����һ��Ŀ��
                //�ӵ�ǰ��дָ��ĺ��濪ʼ���ң������Ǵ�0��ʼ
				//var nextIndex = (WritePoolIndex + i) % writeClients.Length;
                var nextIndex = i;

				//Initialize if not exists
				var existingClient = writeClients[nextIndex];
                //���û��ʵ�����й��쳣
				if (existingClient == null
					|| existingClient.HadExceptions)
				{
					if (existingClient != null && existingClient.IsDisposed == false)
					{
                        //������쳣������Ͽ�����
						existingClient.DisposeConnection();
					}

                    //����һ��EndPoint����
					var nextHost = ReadWriteHosts[nextIndex % ReadWriteHosts.Count];
                    //����һ���ͻ���
					var client = RedisClientFactory.CreateRedisClient(
						nextHost.Host, nextHost.Port);
                    //����һ��Ψһ���
					client.Id = RedisClientCounter++;
					client.ClientManager = this;

                    //���������
					writeClients[nextIndex] = client;

					return client;
				}

				//look for free one
				if (writeClients[nextIndex].Active == false)
				{
                    //�����һ������ģ���ֱ�ӷ���
					return writeClients[nextIndex];
				}
			}
            //����ȫ�������û�к��ʵķ��ؿ�ֵ
			return null;
		}

		/// <summary>
		/// Returns a ReadOnly client using the hosts defined in ReadOnlyHosts.
		/// </summary>
		/// <returns></returns>
		public virtual IRedisClient GetReadOnlyClient()
		{
			lock (readClients)
			{
				AssertValidReadOnlyPool();

				RedisClient inActiveClient;
				while ((inActiveClient = GetInActiveReadClient()) == null)
				{
					Monitor.Wait(readClients);
				}

				ReadPoolIndex++;
				inActiveClient.Active = true;

				//Reset database to default if changed
				if (inActiveClient.Db != Db)
				{
					inActiveClient.Db = Db;
				}

				return inActiveClient;
			}
		}

		/// <summary>
		/// Called within a lock
		/// </summary>
		/// <returns></returns>
		private RedisClient GetInActiveReadClient()
		{
			for (var i=0; i < readClients.Length; i++)
			{
				//var nextIndex = (ReadPoolIndex + i) % readClients.Length;
                var nextIndex = i;
				//Initialize if not exists
				var existingClient = readClients[nextIndex];
				if (existingClient == null
					|| existingClient.HadExceptions)
				{
					if (existingClient != null)
					{
						existingClient.DisposeConnection();
					}

					var nextHost = ReadOnlyHosts[nextIndex % ReadOnlyHosts.Count];
					var client = RedisClientFactory.CreateRedisClient(
						nextHost.Host, nextHost.Port);

					client.ClientManager = this;

					readClients[nextIndex] = client;

					return client;
				}

				//look for free one
				if (!readClients[nextIndex].Active)
				{
					return readClients[nextIndex];
				}
			}
			return null;
		}

        /// <summary>
        /// �����ӳ��йر�һ���ͻ���
        /// </summary>
        /// <param name="client"></param>
		public void DisposeClient(RedisNativeClient client)
		{
            bool hit = false;
            lock (writeClients)
            {
                for (var i = 0; i < writeClients.Length; i++)
                {
                    var writeClient = writeClients[i];

                    if (client != writeClient)
                    {
                        if (writeClient != null && writeClient.Active == false &&
                            DateTime.Now - writeClient.InactiveTime > TimeSpan.FromMinutes(1))
                        {
                            if (writeClient.IsDisposed == false)
                            {
                                writeClient.DisposeConnection();
                            }
                            writeClient = null;
                            Monitor.PulseAll(writeClients);
                        }
                        continue;
                    }
                    hit = true;
                    client.Active = false;
                    client.InactiveTime = DateTime.Now;
                    Monitor.PulseAll(writeClients);
                }
            }

            if (hit == true)
                return;
            

			lock (readClients)
			{
				for (var i = 0; i < readClients.Length; i++)
				{
					var readClient = readClients[i];
                    if (client != readClient)
                    {
                        if (readClient != null && readClient.Active == false &&
                            DateTime.Now - readClient.InactiveTime > TimeSpan.FromMinutes(1))
                        {
                            if (readClient.IsDisposed == false)
                            {
                                readClient.DisposeConnection();
                            }
                            readClient = null; 
                            Monitor.PulseAll(writeClients);
                        }
                        continue; 
                    }
                    hit = true;
					client.Active = false;
                    client.InactiveTime = DateTime.Now;
                    //������
					Monitor.PulseAll(readClients);
					return;
				}
			}

			
			
			//Console.WriteLine("Couldn't find {0} client with Id: {1}, readclients: {2}, writeclients: {3}",
			//    client.IsDisposed ? "Disposed" : "Undisposed",
			//    client.Id,
			//    string.Join(", ", readClients.ToList().ConvertAll(x => x != null ? x.Id.ToString() : "").ToArray()),
			//    string.Join(", ", writeClients.ToList().ConvertAll(x => x != null ? x.Id.ToString() : "").ToArray()));

            if (hit || client.IsDisposed) return;

			throw new NotSupportedException("Cannot add unknown client back to the pool");
		}

		public void Start()
		{
			if (writeClients.Length > 0 || readClients.Length > 0)
				throw new InvalidOperationException("Pool has already been started");

			writeClients = new RedisClient[Config.MaxWritePoolSize];
			WritePoolIndex = 0;

			readClients = new RedisClient[Config.MaxReadPoolSize];
			ReadPoolIndex = 0;
		}

		private void AssertValidReadWritePool()
		{
			if (writeClients.Length < 1)
				throw new InvalidOperationException("Need a minimum read-write pool size of 1, then call Start()");
		}

		private void AssertValidReadOnlyPool()
		{
			if (readClients.Length < 1)
				throw new InvalidOperationException("Need a minimum read pool size of 1, then call Start()");
		}

		~PooledRedisClientManager()
		{
			Dispose(false);
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				// get rid of managed resources
			}

			// get rid of unmanaged resources
			for (var i = 0; i < writeClients.Length; i++)
			{
				Dispose(writeClients[i]);
			}
			for (var i = 0; i < readClients.Length; i++)
			{
				Dispose(readClients[i]);
			}
		}

		protected void Dispose(RedisClient redisClient)
		{
			if (redisClient == null) return;
			try
			{
				redisClient.DisposeConnection();
			}
			catch (Exception ex)
			{
				Log.Error(string.Format(
					"Error when trying to dispose of RedisClient to host {0}:{1}",
					redisClient.Host, redisClient.Port), ex);
			}
		}
	}
}