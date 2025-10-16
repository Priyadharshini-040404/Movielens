using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
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
                _model.LoadUsers();
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

                    // Start execution stopwatch
                    var execSw = Stopwatch.StartNew();

                    // Generate reports (this measures only report generation)
                    var (reports, reportElapsed) = _model.GenerateAllReports(useThreads);

                    // Write CSVs
                    string prefix = useThreads ? "WithThreads" : "WithoutThreads";
                    _model.WriteReportsToCsv(reports, prefix);

                    // Stop execution stopwatch
                    execSw.Stop();

                    // Save report timing to file
                    string timingFile = Path.Combine(_model.ReportsFolder, "timings.txt");
                    File.AppendAllText(timingFile, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} | {prefix} | ReportTimeSeconds: {reportElapsed:F2} | ExecutionTimeSeconds: {execSw.Elapsed.TotalSeconds:F2}{Environment.NewLine}");

                    // Show top 10 in console
                    if (reports.TryGetValue("Top10_General", out var gen)) ConsoleView.ShowTop10($"{prefix} - Top10 General", gen, _model.Movies);
                    if (reports.TryGetValue("Top10_Male", out var male)) ConsoleView.ShowTop10($"{prefix} - Top10 Male", male, _model.Movies);
                    if (reports.TryGetValue("Top10_Female", out var female)) ConsoleView.ShowTop10($"{prefix} - Top10 Female", female, _model.Movies);

                    // Print timings
                    Console.WriteLine($"⏱ Report Generation Time: {reportElapsed:F2} seconds");
                    Console.WriteLine($"⏱ Total Execution Time: {execSw.Elapsed.TotalSeconds:F2} seconds");

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
