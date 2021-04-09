using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Linq;


namespace DatabaseDemo {

public class Queries {
    private DemoDbContext Db;

    public Queries(DemoDbContext db) {
        Db = db;
    }

    // By convention, all public instance methods of which the
    // name starts with `Query` will be used for verifying the
    // correctness of query results.
    //
    // The signature of each `Query*` method should be:
    //
    //     public IQueryable<T> QueryTheNameOfTheTestCase(JsonElement[] args)
    //
    // where the arguments will be bound to the invoked statements.

    public IQueryable<Object> QueryContinents(JsonElement[] args) {
        return
            from region in Db.Regions
            where region.Parent == null
            orderby region.RegionId
            select new {
                Id = region.RegionId,
                ParentId = region.ParentId,
                Name = region.Name,
            };
    }

    public IQueryable<Object> QuerySiblingsAndParents(JsonElement[] args) {
        var leafId = args[0].GetInt64();
        var seed =
            from region
            in Db.Regions
            where region.RegionId == leafId
            select region;

        // This should be a separate method, but it is declared inline
        // instead, so that code analysis can aggregate the complexity
        // of the recursive call and that of the rest of the query.
        // Separate declaration and assignment is needed so that the
        // "use of unassigned local variable" error is prevented.
        Func<IQueryable<Region>, IQueryable<Region>> getRegionPath = null!;

        getRegionPath = children => {
            if (children.Count() == 0) {
                // Empty set must be created using `DbSet`. Otherwise,
                // e.g. `new List<Region>().AsQueryable()` or even
                // `Enumerable.Empty<Region>().AsQueryable()` would result in
                // the exception "The LINQ expression could not be translated".
                return Db.Regions.Take(0);
            }

            var parents =
                from r
                in Db.Regions
                where children.Any(c => c.ParentId == r.RegionId)
                select r;

            return children
                .Union( // siblings
                    from r
                    in Db.Regions
                    where children.Any(c => c.ParentId == r.ParentId)
                    select r
                )
                .Union(parents)
                .Union(getRegionPath(parents)); // recurse
        };

        return
            from region
            in getRegionPath(seed)
            orderby region.ParentId descending, region.RegionId
            select new {
                Id = region.RegionId,
                ParentId = region.ParentId,
                Name = region.Name,
            };
    }

    public IQueryable<Object> QueryNoLoginUsers(JsonElement[] args) {
        return
            from user
            in Db.Users
            where !user.Sessions.Any()
            orderby user.UserId
            select new { UserId = user.UserId };
    }

    public IQueryable<Object> QueryNumValidSessions(JsonElement[] args) {
        return
            from user
            in Db.Users
            orderby user.UserId
            select new {
                UserId = user.UserId,
                ValidSessions = (
                    from session
                    in user.Sessions
                    where session.LogoutEvent == null
                    select 1
                ).Count(),
            };
    }

    public IQueryable<Object> QueryMultiProfileUsers(JsonElement[] args) {
        return
            from user
            in Db.Users
            where user.Profiles.Count > 1
            orderby user.UserId
            select new {
                UserId = user.UserId,
                ProfileCount = user.Profiles.Count,
            };
    }

    public IQueryable<Object> QueryOwnedRealEstateRegionCount(JsonElement[] args) {
        return
            from user
            in Db.Users
            orderby user.UserId
            select new {
                UserId = user.UserId,
                RegionCount = (
                    from realEstate
                    in user.OwnedRealEstates
                    select realEstate.Region.RegionId
                ).Distinct().Count(),
            };
    }

    public IQueryable<Object> QueryProfileCountsByNonGoogleUser(JsonElement[] args) {
        return
            from user
            in Db.Users
            where (
                from p
                in user.Profiles
                where p is GoogleProfile
                select p
            ).Count() == 0
            orderby user.UserId
            select new {
                UserId = user.UserId,
                FbProfileCount = (
                    from p
                    in user.Profiles
                    where p is FacebookProfile
                    select p
                ).Count(),
                InternalProfileCount = (
                    from p
                    in user.Profiles
                    where p is InternalProfile
                    select p
                ).Count(),
            };
    }

    // We have achieved peak enterprise method naming convention
    public IQueryable<Object> QueryAvgDailyPriceByUserByBookingLengthCategory(JsonElement[] args) {
        double ticksPerDay = 24 * 3600 * TimeSpan.TicksPerSecond;
        var existing =
            from b in Db.Bookings
            let days = (double)(b.EndDate.Ticks - b.StartDate.Ticks) / ticksPerDay
            group b
            by new {
                Username = b.User.Username,
                Duration = (
                    days >= 36525 ? "centuries" :
                    days >=   365 ? "years"     :
                    days >=    30 ? "months"    :
                    days >=     7 ? "weeks"     :
                                    "days"
                ),
            }
            into bookings
            select new {
                Username = bookings.Key.Username,
                Duration = bookings.Key.Duration,
                AvgDailyPrice = (
                    (from b in bookings select (double) b.Price).Sum()
                    /
                    (
                        // This abomination needs to be inline because if I pull it
                        // out into a helper function, then it can't be translated.
                        // And we need this workaround in the first place because
                        // the proper solution, i.e. subtracting two `DateTime`s
                        // directly, yields a `TimeSpan`, of which the `Days` etc.
                        // fields are not translated to SQL for some reason.
                        // So we do this in order to achieve DB-side evaluation.
                        from b
                        in bookings
                        select (double)(b.EndDate.Ticks - b.StartDate.Ticks) / ticksPerDay
                    ).Sum()
                ) as double?,
            };

        // Simulate a LEFT OUTER JOIN in this manner,
        // because the usual way of using the `join`
        // clause with `DefaultIfEmpty()` results in
        // an exception being thrown about a `null`
        // value deep down in the guts of EF Core,
        // which is practically impossible to debug.
        var cross =
            from user in Db.Users
            from duration in Db.Durations
            from x in existing
            select new {
                Username = user.Username,
                Duration = duration.Name,
                AvgDailyPrice =
                    x.Username == user.Username && x.Duration == duration.Name
                    ? x.AvgDailyPrice as double?
                    : null,
            };

        return
            from x in cross
            group x.AvgDailyPrice
            by new {
                Username = x.Username,
                Duration = x.Duration,
            }
            into grp
            orderby grp.Key.Username, grp.Key.Duration
            select new {
                Username = grp.Key.Username,
                Duration = grp.Key.Duration,
                AvgDailyPrice = grp.Average(),
            };
    }

    public IQueryable<Object> QueryTopNBookedRegionsForUserX(JsonElement[] args) {
        long userId = args[0].GetInt64();
        int limit = args[1].GetInt32();

        return (
            from user in Db.Users
            where user.UserId == userId
            from booking in user.Bookings
            join realEstate in Db.RealEstates on booking.RealEstate equals realEstate
            group realEstate by realEstate.Region.Name into grp
            orderby grp.Count() descending, grp.Key
            select new {
                RegionName = grp.Key,
                Frequency = grp.Count(),
            }
        ).Take(limit);
    }

    public IQueryable<Object> QueryNorthestBookedLatitudeSlow(JsonElement[] args) {
        // Again, emulate left join by unioning with a set of
        // tuples containing null for the non-key attribute,
        // then collapsing the nulls if and only if other
        // values for the same key also exist in the subset.
        var unsorted = (
            from user in Db.Users
            from booking in Db.Bookings
            from realEstate in Db.RealEstates
            from location in Db.Locations
            where user.UserId == booking.User.UserId
               && booking.RealEstate.RealEstateId == realEstate.RealEstateId
               && location.Region.RegionId == realEstate.Region.RegionId
            group location.Latitude by user.UserId into locations
            select new {
                UserId = locations.Key,
                NorthestLatitude = locations.Max() as double?,
            }
        ).Union(
            from user in Db.Users
            select new {
                UserId = user.UserId,
                NorthestLatitude = null as double?,
            }
        );

        return
            from row in unsorted
            group row.NorthestLatitude by row.UserId into grp
            orderby grp.Key
            select new {
                UserId = grp.Key,
                NorthestLatitude = grp.Max(),
            };
    }

    public IQueryable<Object> QueryNorthestBookedLatitudeFast(JsonElement[] args) {
        var maxLatForRegion =
            from location
            in Db.Locations
            group location.Latitude by location.Region.RegionId into grp
            select new {
                RegionId = grp.Key,
                MaxLatitude = grp.Max(),
            };

        // Again, simulate left join
        var inner =
            from user in Db.Users
            from booking in user.Bookings
            join realEstate in Db.RealEstates on booking.RealEstate equals realEstate
            join maxLat in maxLatForRegion on realEstate.Region.RegionId equals maxLat.RegionId
            group maxLat.MaxLatitude by user.UserId into grp
            select new {
                UserId = grp.Key,
                NorthestLatitude = grp.Max() as double?,
            };

        return
            from x in inner.Union(
                from user in Db.Users select new {
                    UserId = user.UserId,
                    NorthestLatitude = null as double?,
                }
            )
            group x.NorthestLatitude by x.UserId into grp
            orderby grp.Key
            select new {
                UserId = grp.Key,
                NorthestLatitude = grp.Max(),
            };
    }
}

}
