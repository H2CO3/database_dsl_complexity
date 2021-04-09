using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data.SQLite;


namespace DatabaseDemo {

public delegate T RowCallback<T>(SQLiteDataReader reader);

public class DataLoader : IDisposable {
    string DbPath { get; init; }
    SQLiteConnection Conn { get; init; }
    bool IsDisposed { get; set; }

    public DataLoader(string dbPath) {
        DbPath = dbPath;
        Conn = new SQLiteConnection($"Data Source={dbPath}");
        Conn.Open();
        IsDisposed = false;
    }

    public List<T> LoadAll<T>(string sql, RowCallback<T> callback) {
        var cmd = new SQLiteCommand(sql, Conn);
        var reader = cmd.ExecuteReader();
        var rows = new List<T>();

        while (reader.Read()) {
            rows.Add(callback(reader));
        }

        return rows;
    }

    public List<Region> LoadRegions() {
        // Use recursive CTE to query the hierarchy in the correct order,
        // starting with the parents and descending down level-by-level.
        // This is needed for upholding the foreign key constraints when
        // re-inserting the data into the schema of EntityFramework Core.
        var regions = new Dictionary<long, Region>();
        var sql = @"
            WITH tmp AS (
                    SELECT id, name, parent_id, CAST(id AS TEXT) AS path
                    FROM region
                    WHERE parent_id IS NULL
                UNION ALL
                    SELECT region.id, region.name, region.parent_id,
                           (tmp.path || '/' || CAST(region.id AS TEXT)) AS path
                    FROM region
                    INNER JOIN tmp
                    ON region.parent_id = tmp.id
            )
            SELECT id, name, parent_id
            FROM tmp
            ORDER BY path
        ";

        var regionList = LoadAll(sql, reader => {
            var parentId = reader.IsDBNull(2) ? null : reader.GetInt64(2) as long?;
            var parent = reader.IsDBNull(2) ? null : regions[reader.GetInt64(2)];
            var region = new Region {
                RegionId = reader.GetInt64(0),
                Name = reader.GetString(1),
                Bounds = new List<Location>(),
                ParentId = parentId,
                Parent = parent,
            };
            regions[region.RegionId] = region;
            return region;
        });

        // populate regions with locations
        LoadLocations(regionList);

        return regionList;
    }

    public List<Location> LoadLocations(List<Region> regions) {
        var regionsById = regions.ToDictionary(region => region.RegionId, region => region);
        var sql = "SELECT id, latitude, longitude, region_id FROM location ORDER BY region_id, id";
        return LoadAll(sql, reader => {
            var region = regionsById[reader.GetInt64(3)];
            var location = new Location {
                LocationId = reader.GetInt64(0),
                Latitude = reader.GetDouble(1),
                Longitude = reader.GetDouble(2),
                Region = region,
            };
            region.Bounds.Add(location);
            return location;
        });
    }

    public List<Session> LoadSessions() {
        var sql = @"
            SELECT id, user_id,
                   login_public_key, login_date,
                   logout_public_key, logout_date
            FROM session
            ORDER BY user_id, id
        ";

        return LoadAll(sql, reader => {
            var logoutEvent = reader.IsDBNull(4) ? null : new AuthEvent {
                PublicKey = reader.GetByteArray(4),
                Date = reader.GetDateTime(5),
            };
            return new Session {
                SessionId = reader.GetInt64(0),
                UserId = reader.GetInt64(1),
                LoginEvent = new AuthEvent {
                    PublicKey = reader.GetByteArray(2),
                    Date = reader.GetDateTime(3),
                },
                LogoutEvent = logoutEvent,
            };
        });
    }

    public List<Profile> LoadProfiles() {
        var profiles = new List<Profile>();

        foreach (var p in LoadFacebookProfiles()) {
            profiles.Add(p);
        }
        foreach (var p in LoadGoogleProfiles()) {
            profiles.Add(p);
        }
        foreach (var p in LoadInternalProfiles()) {
            profiles.Add(p);
        }

        return profiles;
    }

    public List<FacebookProfile> LoadFacebookProfiles() {
        var sql = @"
            SELECT profile.id, profile.user_id, profile_facebook.facebook_account_id
            FROM profile
            INNER JOIN profile_facebook
            ON profile.facebook_id = profile_facebook.id
            ORDER BY profile.user_id, profile.id
        ";

        return LoadAll(sql, reader => {
            return new FacebookProfile {
                ProfileId = reader.GetInt64(0),
                UserId = reader.GetInt64(1),
                FacebookAccountId = reader.GetString(2),
            };
        });
    }

    public List<GoogleProfile> LoadGoogleProfiles() {
        var sql = @"
            SELECT profile.id,
                   profile.user_id,
                   profile_google.google_account_id,
                   profile_google.email,
                   profile_google.image_url
            FROM profile
            INNER JOIN profile_google
            ON profile.google_id = profile_google.id
            ORDER BY profile.user_id, profile.id
        ";

        return LoadAll(sql, reader => {
            return new GoogleProfile {
                ProfileId = reader.GetInt64(0),
                UserId = reader.GetInt64(1),
                GoogleAccountId = reader.GetString(2),
                EmailAddress = reader.IsDBNull(3) ? null : reader.GetString(3),
                ImageUrl = reader.IsDBNull(4) ? null : reader.GetString(4),
            };
        });
    }

    public List<InternalProfile> LoadInternalProfiles() {
        var sql = @"
            SELECT profile.id,
                   profile.user_id,
                   profile_internal.password_hash,
                   profile_internal.password_salt
            FROM profile
            INNER JOIN profile_internal
            ON profile.internal_id = profile_internal.id
            ORDER BY profile.user_id, profile.id
        ";

        return LoadAll(sql, reader => {
            return new InternalProfile {
                ProfileId = reader.GetInt64(0),
                UserId = reader.GetInt64(1),
                PasswordHash = reader.GetByteArray(2),
                PasswordSalt = reader.GetByteArray(3),
            };
        });
    }

    public List<User> LoadUsers(List<Profile> profiles, List<Session> sessions) {
        var profilesByUser = profiles
            .GroupBy(x => x.UserId)
            .ToDictionary(x => x.Key, x => x.ToList());
        var sessionsByUser = sessions
            .GroupBy(x => x.UserId)
            .ToDictionary(x => x.Key, x => x.ToList());

        var sql = @"
            SELECT id, username, real_name, birth_date
            FROM user
            ORDER BY id
        ";

        // In the two lines below marked "deferred", we do not
        // yet populate the owned real estates and sessions of
        // the user, because the "main" direction of these
        // connections is RealEstate -> Owner and Booking -> User.
        // These two properties are only created for the sake of
        // efficiency, because EF Core generates better queries
        // if we use the list navigation properties than it would
        // if we wrote explicit subqueries.
        return LoadAll(sql, reader => {
            var userId = reader.GetInt64(0);
            return new User {
                UserId = userId,
                Username = reader.GetString(1),
                RealName = reader.IsDBNull(2) ? null : reader.GetString(2),
                BirthDate = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                Profiles = profilesByUser.GetOrDefault(userId, new List<Profile>()),
                Sessions = sessionsByUser.GetOrDefault(userId, new List<Session>()),
                OwnedRealEstates = new List<RealEstate>(), // deferred
                Bookings = new List<Booking>(), // deferred
            };
        });
    }

    public List<RealEstate> LoadRealEstates(List<User> users, List<Region> regions) {
        var usersById = users.ToDictionary(u => u.UserId, u => u);
        var regionsById = regions.ToDictionary(r => r.RegionId, r => r);

        return LoadAll("SELECT * FROM real_estate ORDER BY id", reader => {
            return new RealEstate {
                RealEstateId = reader.GetInt64(0),
                Kind = Enum.Parse<RealEstateKind>(reader.GetString(1), true),
                Owner = usersById[reader.GetInt64(2)],
                Region = regionsById[reader.GetInt64(3)],
            };
        });
    }

    public List<Booking> LoadBookings(List<User> users, List<RealEstate> realEstates) {
        var usersById = users.ToDictionary(u => u.UserId, u => u);
        var realEstatesById = realEstates.ToDictionary(r => r.RealEstateId, r => r);

        return LoadAll("SELECT * FROM booking ORDER BY id", reader => {
            return new Booking {
                BookingId = reader.GetInt64(0),
                RealEstate = realEstatesById[reader.GetInt64(1)],
                User = usersById[reader.GetInt64(2)],
                StartDate = reader.GetDateTime(3),
                EndDate = reader.GetDateTime(4),
                Price = reader.GetDecimal(5),
            };
        });
    }

    public List<Duration> LoadDurations() {
        return new List<Duration> {
            new Duration { DurationId = 1, Name = "days"      },
            new Duration { DurationId = 2, Name = "weeks"     },
            new Duration { DurationId = 3, Name = "months"    },
            new Duration { DurationId = 4, Name = "years"     },
            new Duration { DurationId = 5, Name = "centuries" },
        };
    }

    public void PopulateDatabase(DemoDbContext db) {
        db.Database.EnsureCreated();

        // First, load the example data using raw SQLite
        Console.WriteLine("Loading Data");

        var regions = LoadRegions();
        var profiles = LoadProfiles();
        var sessions = LoadSessions();
        var users = LoadUsers(profiles, sessions);
        var realEstates = LoadRealEstates(users, regions);
        var bookings = LoadBookings(users, realEstates);
        var durations = LoadDurations();

        // Then, initialize and populate the DB managed by the ORM
        Console.WriteLine("Inserting Data");

        db.AddRange(regions);
        db.AddRange(users);
        db.AddRange(realEstates);
        db.AddRange(bookings);
        db.AddRange(durations);

        db.SaveChanges();
    }

    public void Dispose() {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing) {
        if (!IsDisposed) {
            if (disposing) {
                Conn.Dispose();
            }

            IsDisposed = true;
        }
    }

    ~DataLoader() {
        Dispose(false);
    }
}

static class SQLiteDataReaderExtension {
    public static byte[] GetByteArray(this SQLiteDataReader reader, int index) {
        var stream = reader.GetStream(index);
        var maybeMemoryStream = stream as MemoryStream;
        if (maybeMemoryStream != null) {
            return maybeMemoryStream.ToArray();
        }

        var memoryStream = new MemoryStream();
        stream.CopyTo(memoryStream);
        return memoryStream.ToArray();
    }
}

}
