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
    public record User(int UserId, int Age, string Gender);
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
        public string UItemPath => Path.Combine(DataFolder, "u.item");
        public string UUserPath => Path.Combine(DataFolder, "u.user");
        public string UDataPath => Path.Combine(DataFolder, "u.data");
        public Dictionary<int, Movie> Movies { get; private set; } = new();
        public Dictionary<int, User> Users { get; private set; } = new();
        public List<Rating> Ratings { get; private set; } = new();
        public static readonly string[] GenreLabels = new string[]
        {
            "unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary",
            "Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi",
            "Thriller","War","Western"
        };

        public DataModel()
        {
            Directory.CreateDirectory(DataFolder);
            Directory.CreateDirectory(ReportsFolder);
        }
        public void LoadUsers()
        {
            Users.Clear();
            if (!File.Exists(UUserPath)) throw new FileNotFoundException($"Could not find file '{UUserPath}'");

            foreach (var line in File.ReadLines(UUserPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var parts = line.Split(new char[] { '|', '\t' }, StringSplitOptions.None);
                if (parts.Length < 3) continue;
                if (!int.TryParse(parts[0], out int uid)) continue;
                if (!int.TryParse(parts[1], out int age)) continue;
                var gender = parts[2].Trim();
                Users[uid] = new User(uid, age, gender);
            }
        }
        public void LoadMovies()
        {
            Movies.Clear();
            if (!File.Exists(UItemPath)) throw new FileNotFoundException($"Could not find file '{UItemPath}'");

            foreach (var line in File.ReadLines(UItemPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var parts = line.Split('|');
                if (parts.Length < 6) continue;
                if (!int.TryParse(parts[0], out int mid)) continue;
                var title = parts[1].Trim();
                var genres = new List<string>();
                for (int i = 0; i < GenreLabels.Length; i++)
                {
                    int idx = 5 + i;
                    if (idx < parts.Length && parts[idx].Trim() == "1") genres.Add(GenreLabels[i]);
                }
                Movies[mid] = new Movie(mid, title, genres);
            }
        }

        public void LoadRatings()
        {
            Ratings.Clear();
            if (!File.Exists(UDataPath)) throw new FileNotFoundException($"Could not find file '{UDataPath}'");

            foreach (var line in File.ReadLines(UDataPath))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var parts = line.Split('\t');
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
            if (!useThreads)
            {
                return GenerateReportsSequential(sw);
            }
            else
            {
                return GenerateReportsThreaded(sw);
            }
        }
        private (Dictionary<string, List<(int mid, double avg, long cnt)>> reports, double elapsedSeconds)
            GenerateReportsSequential(Stopwatch sw)
        {
            var all = new Dictionary<int, (double sum, long cnt)>();
            var male = new Dictionary<int, (double sum, long cnt)>();
            var female = new Dictionary<int, (double sum, long cnt)>();
            var action = new Dictionary<int, (double sum, long cnt)>();
            var drama = new Dictionary<int, (double sum, long cnt)>();
            var comedy = new Dictionary<int, (double sum, long cnt)>();
            var fantasy = new Dictionary<int, (double sum, long cnt)>();
            var ageLT18 = new Dictionary<int, (double sum, long cnt)>();
            var age18to30 = new Dictionary<int, (double sum, long cnt)>();
            var ageGT30 = new Dictionary<int, (double sum, long cnt)>();

            foreach (var r in Ratings)
            {
                void UpdateDict(Dictionary<int, (double sum, long cnt)> dict)
                {
                    if (!dict.TryGetValue(r.MovieId, out var v)) v = (0, 0);
                    dict[r.MovieId] = (v.sum + r.Score, v.cnt + 1);
                }

                UpdateDict(all);
                if (!Users.TryGetValue(r.UserId, out var user)) continue;
                var g = user.Gender.ToUpperInvariant();
                if (g == "M" || g == "MALE") UpdateDict(male);
                else if (g == "F" || g == "FEMALE") UpdateDict(female);

                if (!Movies.TryGetValue(r.MovieId, out var movie)) continue;
                if (movie.Genres.Contains("Action")) UpdateDict(action);
                if (movie.Genres.Contains("Drama")) UpdateDict(drama);
                if (movie.Genres.Contains("Comedy")) UpdateDict(comedy);
                if (movie.Genres.Contains("Fantasy")) UpdateDict(fantasy);

                if (user.Age < 18) UpdateDict(ageLT18);
                else if (user.Age <= 30) UpdateDict(age18to30);
                else UpdateDict(ageGT30);
            }
            var reports = new Dictionary<string, List<(int mid, double avg, long cnt)>>()
            {
                ["Top10_General"] = TopNFromDict(all, 10),
                ["Top10_Male"] = TopNFromDict(male, 10),
                ["Top10_Female"] = TopNFromDict(female, 10),
                ["Top10_Action"] = TopNFromDict(action, 10),
                ["Top10_Drama"] = TopNFromDict(drama, 10),
                ["Top10_Comedy"] = TopNFromDict(comedy, 10),
                ["Top10_Fantasy"] = TopNFromDict(fantasy, 10),
                ["Top10_Age_LT18"] = TopNFromDict(ageLT18, 10),
                ["Top10_Age_18to30"] = TopNFromDict(age18to30, 10),
                ["Top10_Age_GT30"] = TopNFromDict(ageGT30, 10)
            };

            sw.Stop();
            return (reports, sw.Elapsed.TotalSeconds);
        }
        private (Dictionary<string, List<(int mid, double avg, long cnt)>> reports, double elapsedSeconds)
            GenerateReportsThreaded(Stopwatch sw)
        {
            var total = Ratings.Count;
            int numChunks = (total + ChunkSize - 1) / ChunkSize;

            var allGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var maleGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var femaleGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var actionGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var dramaGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var comedyGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var fantasyGlobal = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var ageLT18Global = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var age18to30Global = new ConcurrentDictionary<int, (double sum, long cnt)>();
            var ageGT30Global = new ConcurrentDictionary<int, (double sum, long cnt)>();

            var tasks = new List<Task>();

            for (int i = 0; i < numChunks; i++)
            {
                int start = i * ChunkSize;
                int end = Math.Min(start + ChunkSize, total);
                var slice = Ratings.GetRange(start, end - start);

                tasks.Add(Task.Run(() =>
                {
                    var all = new Dictionary<int, (double sum, long cnt)>();
                    var male = new Dictionary<int, (double sum, long cnt)>();
                    var female = new Dictionary<int, (double sum, long cnt)>();
                    var action = new Dictionary<int, (double sum, long cnt)>();
                    var drama = new Dictionary<int, (double sum, long cnt)>();
                    var comedy = new Dictionary<int, (double sum, long cnt)>();
                    var fantasy = new Dictionary<int, (double sum, long cnt)>();
                    var ageLT18 = new Dictionary<int, (double sum, long cnt)>();
                    var age18to30 = new Dictionary<int, (double sum, long cnt)>();
                    var ageGT30 = new Dictionary<int, (double sum, long cnt)>();

                    foreach (var r in slice)
                    {
                        void UpdateDict(Dictionary<int, (double sum, long cnt)> dict)
                        {
                            if (!dict.TryGetValue(r.MovieId, out var v)) v = (0, 0);
                            dict[r.MovieId] = (v.sum + r.Score, v.cnt + 1);
                        }

                        UpdateDict(all);
                        if (!Users.TryGetValue(r.UserId, out var user)) continue;
                        var g = user.Gender.ToUpperInvariant();
                        if (g == "M" || g == "MALE") UpdateDict(male);
                        else if (g == "F" || g == "FEMALE") UpdateDict(female);

                        if (!Movies.TryGetValue(r.MovieId, out var movie)) continue;
                        if (movie.Genres.Contains("Action")) UpdateDict(action);
                        if (movie.Genres.Contains("Drama")) UpdateDict(drama);
                        if (movie.Genres.Contains("Comedy")) UpdateDict(comedy);
                        if (movie.Genres.Contains("Fantasy")) UpdateDict(fantasy);

                        if (user.Age < 18) UpdateDict(ageLT18);
                        else if (user.Age <= 30) UpdateDict(age18to30);
                        else UpdateDict(ageGT30);
                    }
                    void Merge(Dictionary<int, (double sum, long cnt)> local, ConcurrentDictionary<int, (double sum, long cnt)> global)
                    {
                        foreach (var kv in local) global.AddOrUpdate(kv.Key, kv.Value, (_, old) => (old.sum + kv.Value.sum, old.cnt + kv.Value.cnt));
                    }
                    Merge(all, allGlobal);
                    Merge(male, maleGlobal);
                    Merge(female, femaleGlobal);
                    Merge(action, actionGlobal);
                    Merge(drama, dramaGlobal);
                    Merge(comedy, comedyGlobal);
                    Merge(fantasy, fantasyGlobal);
                    Merge(ageLT18, ageLT18Global);
                    Merge(age18to30, age18to30Global);
                    Merge(ageGT30, ageGT30Global);
                }));
            }
            Task.WaitAll(tasks.ToArray());

            var reports = new Dictionary<string, List<(int mid, double avg, long cnt)>>()
            {
                ["Top10_General"] = TopNFromDict(allGlobal, 10),
                ["Top10_Male"] = TopNFromDict(maleGlobal, 10),
                ["Top10_Female"] = TopNFromDict(femaleGlobal, 10),
                ["Top10_Action"] = TopNFromDict(actionGlobal, 10),
                ["Top10_Drama"] = TopNFromDict(dramaGlobal, 10),
                ["Top10_Comedy"] = TopNFromDict(comedyGlobal, 10),
                ["Top10_Fantasy"] = TopNFromDict(fantasyGlobal, 10),
                ["Top10_Age_LT18"] = TopNFromDict(ageLT18Global, 10),
                ["Top10_Age_18to30"] = TopNFromDict(age18to30Global, 10),
                ["Top10_Age_GT30"] = TopNFromDict(ageGT30Global, 10)
            };
            sw.Stop();
            return (reports, sw.Elapsed.TotalSeconds);
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
