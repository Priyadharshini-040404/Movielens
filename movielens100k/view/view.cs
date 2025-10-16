using System;
using System.Collections.Generic;
using MovieLensOLAP_MVC.Models;
namespace MovieLensOLAP_MVC.View
{
    public static class ConsoleView
    {
        public static void PrintHeader()
        {
            Console.Clear();
            Console.WriteLine("MovieLens OLAP Report Generator (MVC Console)");
            Console.WriteLine("============================================");
            Console.WriteLine($"Data folder: {AppDomain.CurrentDomain.BaseDirectory}Data");
            Console.WriteLine($"Reports folder: {AppDomain.CurrentDomain.BaseDirectory}Reports");
            Console.WriteLine();
        }
        public static void PrintMenu()
        {
            Console.WriteLine("Menu:");
            Console.WriteLine("1. Generate reports WITHOUT threads");
            Console.WriteLine("2. Generate reports WITH threads (10k chunk per task)");
            Console.WriteLine("3. Exit");
            Console.Write("Enter choice: ");
        }
        public static void ShowTop10(string title, List<(int mid, double avg, long cnt)> rows, Dictionary<int, Movie> movies)
        {
            Console.WriteLine();
            Console.WriteLine($"--- {title} ---");
            Console.WriteLine($"Rank\tMovieId\tAvg\tCount\tTitle");
            int r = 1;
            foreach (var row in rows)
            {
                string t = movies.TryGetValue(row.mid, out var m) ? m.Title : "Unknown";
                Console.WriteLine($"{r}\t{row.mid}\t{row.avg:F3}\t{row.cnt}\t{t}");
                r++;
            }
            Console.WriteLine();
        }
        public static void PrintTiming(double seconds)
        {
            Console.WriteLine($"Time taken: {seconds:F2} seconds");
        }

        public static void Pause()
        {
            Console.WriteLine("Press Enter to continue...");
            Console.ReadLine();
        }
    }
}
