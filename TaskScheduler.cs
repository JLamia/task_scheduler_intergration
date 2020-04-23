#nullable enable
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection; 


namespace ParallelGraph
{
    public class MyTaskScheduler : TaskScheduler
    {
        private MyThreadPool _mThreadPool;
        private static int _defaultNumOfThreads = Environment.ProcessorCount;
        public new int Id { get; }

        public MyTaskScheduler(int i, int numOfThreads, Exception argumentException)
        {
            _mThreadPool = new MyThreadPool(numOfThreads);
            Id = i;
            ArgumentException = argumentException;
        }

        public MyTaskScheduler(int i, Exception argumentException)
        {
            ArgumentException = argumentException;
            _mThreadPool = new MyThreadPool(_defaultNumOfThreads);
        }

        protected override IEnumerable<Task>? GetScheduledTasks()
        {
            return _mThreadPool.GetWorkQueueTasks();
        }

        private static readonly ParameterizedThreadStart LongRunningThreadWork = s =>
        {
            Debug.Assert(s is Task);
            var executeMethod = typeof(Task).GetMethod("ExecuteWithThreadLocal",
                BindingFlags.Instance | BindingFlags.NonPublic);
            //((Task)s).ExecuteEntryUnsafe(threadPoolThread: null);
            executeMethod.Invoke((Task) s, new object[] {null});
        };

        protected override void QueueTask(Task task)
        {
            if (task == null)
                throw ArgumentException;
            var options = task.CreationOptions;
            if ((options & TaskCreationOptions.LongRunning) != 0)
            {
                Thread thread = new Thread(LongRunningThreadWork);
                thread.IsBackground = true;
                thread.Start(task);
            }
            else
            {
               _mThreadPool.QueueUserTask(task);
            }
        }

        public Exception ArgumentException { get ; set; }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
        {
            throw new NotImplementedException();
        }
    }

    public class MyThreadPool : IDisposable
    {

        private  Queue<Task> _workQueue;
        private  List<Thread> _threads;
        private int _numberofThreads;
        private ManualResetEvent _stopEvent;
        private object _stopLock;
        private bool _isDisposed;
        private Thread _scheduleThread;
        private ManualResetEvent _scheduleEvent;
        private Dictionary<int, ManualResetEvent> _threadsEvent;

        public void QueueUserTask(Task task)
        {
            if (task == null)
                throw new ArgumentException("Task can't be null!");
            lock (_stopLock)
                _workQueue.Enqueue(task);
        }
        
        public MyThreadPool(int numberOfThreads)
        {
            if (numberOfThreads <= 0)
                throw new ArgumentException("Number of threads must be positive");
            
            _numberofThreads = numberOfThreads;
            _threadsEvent = new Dictionary<int, ManualResetEvent>();

            _workQueue = new Queue<Task>();
            _threads = new List<Thread>();
            _stopLock = new object();
            
            _stopEvent = new ManualResetEvent(false);
            _scheduleEvent = new ManualResetEvent(false);

            _isDisposed = false;
            
            _scheduleThread = new Thread(FindAndStartFreeThread) {IsBackground = true};

            for (int i = 0; i < _numberofThreads; i++)
            {
                var thread = new Thread(() => ThreadWork(i));
                _threads.Add(thread);
                _threads[i].Start();
            }
        }

        private void FindAndStartFreeThread()
        {
            for (;;)
            {
                _scheduleEvent.WaitOne();
                lock (_threads)
                {
                    foreach (var thread in _threads)
                    {
                        if (_threadsEvent[thread.ManagedThreadId].WaitOne() == false)
                        {
                            _threadsEvent[thread.ManagedThreadId].Set();
                            break;
                        }
                    }
                }

                _scheduleEvent.Reset();
            }
        }
        
        private void ThreadWork(int i)
        {
            var lockObj = new object();
            lock (lockObj)
            {
                while (true)
                {
                    if (_workQueue.Count == 0) continue;
                    Console.WriteLine($"Thread number {i}, starts task");
                    var task = _workQueue.Dequeue();
                    if (task == null) continue;
                    var threadCallMethod = typeof(Task).GetMethod("ExecuteWithThreadLocal",
                        BindingFlags.Instance | BindingFlags.NonPublic);
                    threadCallMethod.Invoke(this, new object[] {task, _threads[i]});
                    //task.ExecuteFromThreadPool(threads[i]);
                }
            }
        }

        private void RemoveTask()
        {
            lock (_workQueue)
                _workQueue.Dequeue();
        }

        public IEnumerable<Task>? GetWorkQueueTasks()
        {
            return _workQueue;
        }

        ~MyThreadPool()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            _stopEvent.Dispose();
            _scheduleEvent.Dispose();
        }
        
        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _scheduleThread.Abort();
                    _scheduleEvent.Dispose();

                    for (int i = 0; i < _numberofThreads; i++)
                    {
                        _threads[i].Abort();
                        _threadsEvent[_threads[i].ManagedThreadId].Dispose();
                    }
                }
                _isDisposed = true;
            }
        }
    }
}