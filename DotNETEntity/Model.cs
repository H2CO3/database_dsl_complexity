using System;
using System.Text;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Proxies;

namespace DatabaseDemo {

public class DemoDbContext : DbContext {
    private string DbPath { get; init; }

    public DbSet<User> Users { get; set; } = null!;
    public DbSet<Profile> Profiles { get; set; } = null!;
    public DbSet<FacebookProfile> FacebookProfiles { get; set; } = null!;
    public DbSet<GoogleProfile> GoogleProfiles { get; set; } = null!;
    public DbSet<InternalProfile> InternalProfiles { get; set; } = null!;
    public DbSet<Session> Sessions { get; set; } = null!;
    public DbSet<AuthEvent> AuthEvents { get; set; } = null!;
    public DbSet<RealEstate> RealEstates { get; set; } = null!;
    public DbSet<Region> Regions { get; set; } = null!;
    public DbSet<Location> Locations { get; set; } = null!;
    public DbSet<Booking> Bookings { get; set; } = null!;
    public DbSet<Duration> Durations { get; set; } = null!;

    public DemoDbContext(string dbPath) : base() {
        DbPath = dbPath;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder options) {
        options
            .UseLazyLoadingProxies()
            .UseSqlite($"Data Source={DbPath}");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder) {
        modelBuilder.Entity<User>()
            .HasIndex(u => u.Username)
            .IsUnique();

        modelBuilder.Entity<User>()
            .HasIndex(u => u.BirthDate);

        modelBuilder.Entity<AuthEvent>()
            .HasIndex(e => e.PublicKey);
    }
}

public class User {
    public long UserId { get; set; }
    public string Username { get; set; } = null!;
    public string? RealName { get; set; }
    public DateTime? BirthDate { get; set; }
    public virtual List<Profile> Profiles { get; set; } = null!;
    public virtual List<Session> Sessions { get; set; } = null!;

    [InverseProperty("Owner")]
    public virtual List<RealEstate> OwnedRealEstates { get; set; } = null!;
    [InverseProperty("User")]
    public virtual List<Booking> Bookings { get; set; } = null!;

    public override string ToString() {
        return $"User {{ UserId = {UserId}, Username = {Username}, RealName = {RealName}, BirthDate = {BirthDate}, Profiles.Count = {Profiles.Count}, Sessions.Count = {Sessions.Count} }}";
    }
}

public class Profile {
    public long ProfileId { get; set; }
    public long UserId { get; set; }
}

public class FacebookProfile : Profile {
    public string FacebookAccountId { get; set; } = null!;

    public override string ToString() {
        return $"FacebookProfile {{ ProfileId = {ProfileId}, User = {UserId}, FacebookAccountId = {FacebookAccountId} }}";
    }
}

public class GoogleProfile : Profile {
    public string GoogleAccountId { get; set; } = null!;
    public string? EmailAddress { get; set; }
    public string? ImageUrl { get; set; }

    public override string ToString() {
        return $"GoogleProfile {{ ProfileId = {ProfileId}, User = {UserId}, GoogleAccountId = {GoogleAccountId}, EmailAddress = {EmailAddress}, ImageUrl = {ImageUrl} }}";
    }
}

public class InternalProfile : Profile {
    public byte[] PasswordHash { get; set; } = {};
    public byte[] PasswordSalt { get; set; } = {};

    public override string ToString() {
        var sb = new StringBuilder("InternalProfile { ");

        sb.AppendFormat("ProfileId = {0}, User = {1}, PasswordHash = ", ProfileId, UserId);

        foreach (byte b in PasswordHash) {
            sb.AppendFormat("{0:X02}", b);
        }

        sb.Append(", PasswordSalt = ");

        foreach (byte b in PasswordSalt) {
            sb.AppendFormat("{0:X02}", b);
        }

        sb.Append(" }");

        return sb.ToString();
    }
}

public class Session {
    public long SessionId { get; set; }
    public long UserId { get; set; }
    public virtual AuthEvent LoginEvent { get; set; } = null!;
    public virtual AuthEvent? LogoutEvent { get; set; }

    public override string ToString() {
        return $"Session {{ SessionId = {SessionId}, User = {UserId}, LoginEvent = {LoginEvent}, LogoutEvent = {LogoutEvent} }}";
    }
}

public class AuthEvent {
    public long AuthEventId { get; set; }
    public byte[] PublicKey { get; set; } = {};
    public DateTime Date { get; set; }

    public override string ToString() {
        var sb = new StringBuilder("AuthEvent { PublicKey = ");

        foreach (byte b in PublicKey) {
            sb.AppendFormat("{0:X02}", b);
        }

        sb.AppendFormat(", Date = {0} }}", Date);
        return sb.ToString();
    }
}

public enum RealEstateKind {
    Apartment,
    House,
    Mansion,
    Penthouse,
}

public class RealEstate {
    public long RealEstateId { get; set; }
    public RealEstateKind Kind { get; set; }
    public virtual User Owner { get; set; } = null!;
    public virtual Region Region { get; set; } = null!;

    public override string ToString() {
        return $"RealEstate {{ RealEstateId = {RealEstateId}, Kind = {Kind}, Owner = {Owner.UserId}, Region = {Region.RegionId} }}";
    }
}

public class Region {
    public long RegionId { get; set; }
    public string Name { get; set; } = null!;
    [InverseProperty("Region")]
    public virtual List<Location> Bounds { get; set; } = null!;
    public virtual long? ParentId { get; set; }
    [ForeignKey("ParentId")]
    public virtual Region? Parent { get; set; }

    public override string ToString() {
        return $"Region {{ RegionId = {RegionId}, Name = {Name}, Bounds.Count = {Bounds.Count}, Parent = {Parent?.RegionId} }}";
    }
}

public class Location {
    public long LocationId { get; set; }
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    [InverseProperty("Bounds")]
    public virtual Region Region { get; set; } = null!;

    public override string ToString() {
        return $"Location {{ LocationId = {LocationId}, Latitude = {Latitude}, Longitude = {Longitude}, Region = {Region.RegionId} }}";
    }
}

public class Booking {
    public long BookingId { get; set; }
    public virtual RealEstate RealEstate { get; set; } = null!;
    public virtual User User { get; set; } = null!;
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public Decimal Price { get; set; }

    public override string ToString() {
        return $"Booking {{ BookingId = {BookingId}, RealEstate = {RealEstate.RealEstateId}, User = {User.UserId}, StartDate = {StartDate}, EndDate = {EndDate}, Price = {Price} }}";
    }
}

// This is needed in place of a temporary `VALUES (...), ...` table
// for the `AvgDailyPriceByUserByBookingLengthCategory()` query.
public class Duration {
    public long DurationId { get; set; }
    public String Name { get; set; } = String.Empty;
}

}
