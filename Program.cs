using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace ParallelGraph
{
    
    public class Node<T>
    {
        public T Val { get; private set; }
        public readonly int Ind;

        public Node(T val, int ind)
        {
            Val = val;
            Ind = ind;
        }
    }
    public class Graph<T>
    {
        private readonly List<Node<T>>[] _adjacencyList;
        private readonly List<Node<T>>[] _parentsList;
        private readonly Node<T>[] _allNodes;
        public readonly int Size;

        public Node<T> GetNode(int ind)
        {
            return _allNodes[ind];
        }

        public List<Node<T>> GetParents(Node<T> node)
        {
            return _parentsList[node.Ind];
        }


        public int GetParentsCount(Node<T> node)
        {
            return _parentsList[node.Ind].Count;
        }

        public int GetParentsCount(int ind)
        {
            return _parentsList[ind].Count;
        }

        public List<Node<T>> GetChildren(Node<T> node)
        {
            return _adjacencyList[node.Ind];
        }


        public Graph(int size)
        {
            Size = size;
            _adjacencyList = new List<Node<T>>[Size];
            _parentsList = new List<Node<T>>[Size];
            _allNodes = new Node<T>[Size];
        }

        public Graph(int size, Node<T>[] nodes, List<Tuple<int, int>> edges)
        {
            Size = size;

            _allNodes = new Node<T>[Size];
            _parentsList = new List<Node<T>>[Size];
            _adjacencyList = new List<Node<T>>[Size];

            for (int i = 0; i < Size; i++)
            {
                _parentsList[i] = new List<Node<T>>();
                _adjacencyList[i] = new List<Node<T>>();
            }

            _allNodes = nodes;

            foreach (var edge in edges)
            {
                var v = edge.Item1;
                var u = edge.Item2;
                _adjacencyList[v].Add(_allNodes[u]);
                _parentsList[u].Add(_allNodes[v]);
            }
        }
    }

    class Program
    {
        private static async Task ProcessInParallelAsync(Graph<Action> actions)
        {
            //var ts = new myTaskScheduler(1);
            var factory = new TaskFactory();
            var allTasks = new List<Task>();
            var map = new Dictionary<Task, Node<Action>>();
            var finishedActions = new bool[actions.Size];

            for (var i = 0; i < actions.Size; i++)
            {
                if (actions.GetParentsCount(i) != 0) continue;
                var task = factory.StartNew(actions.GetNode(i).Val);
                allTasks.Add(task);
               // var opts = task.CreationOptions;
                map[task] = actions.GetNode(i);
            }

            while (allTasks.Any())
            {
                //Console.WriteLine("Come to tasks processing");
                var finished = await Task.WhenAny(allTasks);
               // Console.WriteLine("Smth has finished");
                Console.WriteLine(allTasks.Count);
                for (var i = 0; i < allTasks.Count; i++)
                {
                    var curTask = allTasks[i];
                    if (finished != curTask) continue;
                    var node = map[curTask];
                    finishedActions[node.Ind] = true;
                    foreach (var child in actions.GetChildren(node))
                    {
                        var canBeStarted = true;
                        foreach (var parent in actions.GetParents(child)
                            .Where(parent => !finishedActions[parent.Ind]))
                        {
                            canBeStarted = false;
                        }

                        if (!canBeStarted) continue;
                        var newTask = factory.StartNew(child.Val);
                        map[newTask] = child;
                        allTasks.Add(newTask);
                        Console.WriteLine("Task {0} is started", child.Ind + 1);
                        //factory.StartNew(map[allTasks.Last()]);
                    }
                }
                allTasks.Remove(finished);
            }
        }
        public static async Task Main()
        {
            Action a1 = () => { Console.WriteLine("1"); };
            Action a2 = () => { Console.WriteLine("2"); Thread.Sleep(200);};
            Action a3 = () => { Console.WriteLine("3"); Thread.Sleep(300);};
            var nodes = new Node<Action>[3];
            nodes[0] = new Node<Action>(a1, 0);
            nodes[1] = new Node<Action>(a2, 1);
            nodes[2] = new Node<Action>(a3, 2);
            List<Tuple<int, int>> lst = new List<Tuple<int, int>>();
            lst.Add(new Tuple<int, int>(0, 1));
            lst.Add(new Tuple<int, int>(0, 2));
            var graph = new Graph<Action>(3, nodes, lst);
            await ProcessInParallelAsync(graph);
        }
    }
}