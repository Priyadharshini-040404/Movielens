using System;
using System.IO;
using System.Collections.Generic;
using MovieLensOLAP_MVC.Models;
using MovieLensOLAP_MVC.View;

namespace MovieLensOLAP_MVC.Controllers
{
    public class Controller
    {
        private readonly DataModel _model;

        public Controller()
        {
            _model = new DataModel();
        }

        public void Run()
        {
            ConsoleView.PrintHeader();

            try
            {
                Console.WriteLine("Loading data...");
                _model.LoadMovies();
                _model.LoadRatings();
                Console.WriteLine("Data loaded successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error loading data: {ex.Message}");
                return;
            }

            while (true)
            {
                ConsoleView.PrintMenu();
                var choice = Console.ReadLine()?.Trim();
                if (choice == "3") break;

                if (choice == "1" || choice == "2")
                {
                    bool useThreads = choice == "2";
                    Console.WriteLine(useThreads ? "Generating WITH threads..." : "Generating WITHOUT threads...");
                    var (reports, elapsed) = _model.GenerateAllReports(useThreads);

                    string prefix = useThreads ? "WithThreads" : "WithoutThreads";
                    _model.WriteReportsToCsv(reports, prefix);

                    string timingFile = Path.Combine(_model.ReportsFolder, "timings.txt");
                    File.AppendAllText(timingFile, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} | {prefix} | TimeSeconds: {elapsed:F2}{Environment.NewLine}");

                    foreach (var kv in reports)
                    {
                        ConsoleView.ShowTop10($"{prefix} - {kv.Key}", kv.Value, _model.Movies);
                    }

                    ConsoleView.PrintTiming(elapsed);
                    Console.WriteLine($"All report CSVs saved to: {_model.ReportsFolder}");
                    ConsoleView.Pause();
                    ConsoleView.PrintHeader();
                }
                else
                {
                    Console.WriteLine("Invalid choice. Try again.");
                }
            }

            Console.WriteLine("Exiting. Goodbye!");
        }
    }
}
