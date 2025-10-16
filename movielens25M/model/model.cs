using System;
using System.IO;
using System.Linq;
using System.Globalization;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Text;
using System.Diagnostics;
namespace MovieLensOLAP_MVC.Models
{
    public record Movie(int MovieId, string Title, List<string> Genres);
    public record Rating(int UserId, int MovieId, double Score);

    public class DataModel
    {
        public string DataFolder { get; init; } =
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "Data"));
        public string ReportsFolder { get; init; } =
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "Reports"));
        public int ChunkSize { get; init; } = 10000;
        public int MinVotesThreshold { get; init; } = 5;
        public string MoviesPath => Path.Combine(DataFolder, "movies.csv");
        public string RatingsPath => Path.Combine(DataFolder, "ratings.csv");
        public Dictionary<int, Movie> Movies { get; private set; } = new();
        public List<Rating> Ratings { get; private set; } = new();
        public static readonly string[] ReportGenres = new string[] { "Action", "Drama", "Comedy", "Fantasy" };
        public DataModel()
        {
            Directory.CreateDirectory(DataFolder);
            Directory.CreateDirectory(ReportsFolder);
        }
        public void LoadMovies()
        {
            Movies.Clear();
            if (!File.Exists(MoviesPath)) throw new FileNotFoundException($"Could not find file '{MoviesPath}'");

            foreach (var line in File.ReadLines(MoviesPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("movieId")) continue; // skip header
                var parts = line.Split(',');
                if (parts.Length < 3) continue;
                if (!int.TryParse(parts[0], out int mid)) continue;
                var title = parts[1].Trim();
                var genres = parts[2].Split('|', StringSplitOptions.RemoveEmptyEntries).Select(g => g.Trim()).ToList();
                Movies[mid] = new Movie(mid, title, genres);
            }
        }
        public void LoadRatings()
        {
            Ratings.Clear();
            if (!File.Exists(RatingsPath)) throw new FileNotFoundException($"Could not find file '{RatingsPath}'");

            foreach (var line in File.ReadLines(RatingsPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("userId")) continue;
                var parts = line.Split(',');
                if (parts.Length < 3) continue;
                if (!int.TryParse(parts[0], out int uid)) continue;
                if (!int.TryParse(parts[1], out int mid)) continue;
                if (!double.TryParse(parts[2], NumberStyles.Any, CultureInfo.InvariantCulture, out double score)) continue;
                Ratings.Add(new Rating(uid, mid, score));
            }
        }
        private List<(int mid, double avg, long cnt)> TopNFromDict(IDictionary<int, (double sum, long cnt)> dict, int n)
        {
            return dict.Where(kv => kv.Value.cnt >= MinVotesThreshold)
                       .Select(kv => (kv.Key, kv.Value.sum / kv.Value.cnt, kv.Value.cnt))
                       .OrderByDescending(x => x.Item2)
                       .ThenByDescending(x => x.Item3)
                       .ThenBy(x => x.Item1)
                       .Take(n)
                       .ToList();
        }
        public (Dictionary<string, List<(int mid, double avg, long cnt)>> reports, double elapsedSeconds)
            GenerateAllReports(bool useThreads)
        {
            var sw = Stopwatch.StartNew();
            var reports = useThreads ? GenerateReportsThreaded() : GenerateReportsSequential();
            sw.Stop();
            return (reports, sw.Elapsed.TotalSeconds);
        }
        private Dictionary<string, List<(int mid, double avg, long cnt)>> GenerateReportsSequential()
        {
            var all = new Dictionary<int, (double sum, long cnt)>();
            var genreDicts = ReportGenres.ToDictionary(g => g, g => new Dictionary<int, (double sum, long cnt)>());

            foreach (var r in Ratings)
            {
                void UpdateDict(Dictionary<int, (double sum, long cnt)> dict)
                {
                    if (!dict.TryGetValue(r.MovieId, out var v)) v = (0, 0);
                    dict[r.MovieId] = (v.sum + r.Score, v.cnt + 1);
                }

                UpdateDict(all);

                if (!Movies.TryGetValue(r.MovieId, out var movie)) continue;

                foreach (var genre in ReportGenres)
                    if (movie.Genres.Contains(genre)) UpdateDict(genreDicts[genre]);
            }
            var reports = new Dictionary<string, List<(int mid, double avg, long cnt)>>()
            {
                ["Top10_General"] = TopNFromDict(all, 10)
            };
            foreach (var genre in ReportGenres)
                reports[$"Top10_{genre}"] = TopNFromDict(genreDicts[genre], 10);

            return reports;
        }
        private Dictionary<string, List<(int mid, double avg, long cnt)>> GenerateReportsThreaded()
        {
            int total = Ratings.Count;
            int numChunks = (total + ChunkSize - 1) / ChunkSize;

            var allGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var genreGlobals = ReportGenres.ToDictionary(g => g, g => new ConcurrentDictionary<int, (double sum, long cnt)>());

            var tasks = new List<Task>();

            for (int i = 0; i < numChunks; i++)
            {
                int start = i * ChunkSize;
                int end = Math.Min(start + ChunkSize, total);
                var slice = Ratings.GetRange(start, end - start);

                tasks.Add(Task.Run(() =>
                {
                    var allLocal = new Dictionary<int, (double sum, long cnt)>();
                    var genreLocals = ReportGenres.ToDictionary(g => g, g => new Dictionary<int, (double sum, long cnt)>());

                    foreach (var r in slice)
                    {
                        void UpdateDict(Dictionary<int, (double sum, long cnt)> dict)
                        {
                            if (!dict.TryGetValue(r.MovieId, out var v)) v = (0, 0);
                            dict[r.MovieId] = (v.sum + r.Score, v.cnt + 1);
                        }

                        UpdateDict(allLocal);

                        if (!Movies.TryGetValue(r.MovieId, out var movie)) continue;

                        foreach (var genre in ReportGenres)
                            if (movie.Genres.Contains(genre)) UpdateDict(genreLocals[genre]);
                    }

                    void Merge(Dictionary<int, (double sum, long cnt)> local, ConcurrentDictionary<int, (double sum, long cnt)> global)
                    {
                        foreach (var kv in local)
                            global.AddOrUpdate(kv.Key, kv.Value, (_, old) => (old.sum + kv.Value.sum, old.cnt + kv.Value.cnt));
                    }

                    Merge(allLocal, allGlobal);
                    foreach (var genre in ReportGenres)
                        Merge(genreLocals[genre], genreGlobals[genre]);
                }));
            }
            Task.WaitAll(tasks.ToArray());

            var reports = new Dictionary<string, List<(int mid, double avg, long cnt)>>()
            {
                ["Top10_General"] = TopNFromDict(allGlobal, 10)
            };
            foreach (var genre in ReportGenres)
                reports[$"Top10_{genre}"] = TopNFromDict(genreGlobals[genre], 10);

            return reports;
        }
        public void WriteReportsToCsv(Dictionary<string, List<(int mid, double avg, long cnt)>> reports, string prefix)
        {
            Directory.CreateDirectory(ReportsFolder);
            foreach (var kv in reports)
            {
                string file = Path.Combine(ReportsFolder, $"{prefix}_{kv.Key}.csv");
                using var sw = new StreamWriter(file, false, Encoding.UTF8);
                sw.WriteLine("Rank,MovieId,Title,AvgRating,NumRatings");
                int rank = 1;
                foreach (var row in kv.Value)
                {
                    string title = Movies.TryGetValue(row.mid, out var m) ? m.Title.Replace(",", " ") : "Unknown";
                    sw.WriteLine($"{rank},{row.mid},\"{title}\",{row.avg:F3},{row.cnt}");
                    rank++;
                }
            }
        }
    }
}
