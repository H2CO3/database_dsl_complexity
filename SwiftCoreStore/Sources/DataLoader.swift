import Foundation
import CoreStore

/// Class for retrieving data from the raw SQLite
/// example database and putting it into CoreStore.
final class DataLoader {
    private let sqlDB: SQLiteDB
    private let dataStack: DataStack

    init(sqlDB: SQLiteDB, dataStack: DataStack) {
        self.sqlDB = sqlDB
        self.dataStack = dataStack
    }

    func load() throws {
        try dataStack.perform(synchronous: {
            let users = try loadUsers(transaction: $0)
            let regions = try loadRegions(transaction: $0)
            let realEstates = try loadRealEstates(transaction: $0, users: users, regions: regions)
            try loadBookings(transaction: $0, users: users, realEstates: realEstates)
        })
    }

    private func loadProfiles(transaction: SynchronousDataTransaction) throws -> [Int64: [Profile]] {
        let profileDicts = try sqlDB.query("""
        SELECT profile.*,
               profile_facebook.facebook_account_id,
               profile_google.google_account_id,
               profile_google.email,
               profile_google.image_url,
               profile_internal.password_hash,
               profile_internal.password_salt
        FROM profile
        LEFT JOIN profile_facebook
          ON profile.facebook_id = profile_facebook.id
        LEFT JOIN profile_google
          ON profile.google_id = profile_google.id
        LEFT JOIN profile_internal
          ON profile.internal_id = profile_internal.id
        """)

        let profileDictsByUser = Dictionary(grouping: profileDicts, by: {
            $0["user_id"] as! Int64
        })
        var profilesByUser = [Int64: [Profile]](minimumCapacity: profileDictsByUser.count)

        for (userID, profileDicts) in profileDictsByUser {
            var profiles = [Profile]()
            profiles.reserveCapacity(profileDicts.count)

            for p in profileDicts {
                let profile: Profile

                if p["facebook_id"]! != nil {
                    let tmp = transaction.create(Into<FacebookProfile>())
                    tmp.accountID = p["facebook_account_id"] as! String
                    profile = tmp
                } else if p["google_id"]! != nil {
                    let tmp = transaction.create(Into<GoogleProfile>())
                    tmp.accountID = p["google_account_id"] as! String
                    tmp.email = p["email"] as? String
                    tmp.imageURL = (p["image_url"] as? String).flatMap(URL.init(string:))
                    profile = tmp
                } else if p["internal_id"]! != nil {
                    let tmp = transaction.create(Into<InternalProfile>())
                    tmp.passwordHash = p["password_hash"] as! Data
                    tmp.passwordSalt = p["password_salt"] as! Data
                    profile = tmp
                } else {
                    throw SQLiteError.queryFailed(reason: "invalid Profile variant")
                }

                profiles.append(profile)
            }

            profilesByUser[userID] = profiles
        }

        return profilesByUser
    }

    private func loadSessions(transaction: SynchronousDataTransaction) throws -> [Int64: [Session]] {
        let sessionDicts = try sqlDB.query("""
        SELECT user_id,
               login_public_key, login_date,
               logout_public_key, logout_date
        FROM session
        """)

        let sessionDictsByUser = Dictionary(grouping: sessionDicts, by: {
            $0["user_id"] as! Int64
        })
        var sessionsByUser = [Int64: [Session]](minimumCapacity: sessionDictsByUser.count)

        for (userID, sessionDicts) in sessionDictsByUser {
            var sessions = [Session]()
            sessions.reserveCapacity(sessionDicts.count)

            for s in sessionDicts {
                let login = transaction.create(Into<AuthEvent>())
                login.publicKey = s["login_public_key"] as! Data
                login.date = sqlDB.dateFormatter.date(from: s["login_date"] as! String)!

                let logout: AuthEvent?
                if let logoutPubkey = s["logout_public_key"] as? Data,
                   let logoutDate = s["logout_date"] as? String
                {
                    let tmp = transaction.create(Into<AuthEvent>())
                    tmp.publicKey = logoutPubkey
                    tmp.date = sqlDB.dateFormatter.date(from: logoutDate)!
                    logout = tmp
                } else {
                    logout = nil
                }

                let session = transaction.create(Into<Session>())
                session.login = login
                session.logout = logout
                sessions.append(session)
            }

            sessionsByUser[userID] = sessions
        }

        return sessionsByUser
    }

    private func loadUsers(transaction: SynchronousDataTransaction) throws -> [Int64: User] {
        let profiles = try loadProfiles(transaction: transaction)
        let sessions = try loadSessions(transaction: transaction)

        let userDicts = try sqlDB.query("""
        SELECT id, username, real_name, birth_date
        FROM user
        """)
        var usersByID = [Int64: User](minimumCapacity: userDicts.count)

        for u in userDicts {
            let user = transaction.create(Into<User>())

            user.userID = u["id"] as! Int64
            user.username = u["username"] as! String
            user.realName = u["real_name"] as? String
            user.birthDate = (u["birth_date"] as? String).flatMap(sqlDB.dateFormatter.date(from:))
            user.profiles = profiles[user.userID] ?? []
            user.sessions = sessions[user.userID] ?? []

            usersByID[user.userID] = user
        }

        return usersByID
    }

    private func loadLocations(transaction: SynchronousDataTransaction) throws -> [Int64: [Location]] {
        let locationDicts = try sqlDB.query(
            "SELECT latitude, longitude, region_id FROM location ORDER BY region_id"
        )
        let locationDictsByRegion = Dictionary(grouping: locationDicts, by: {
            $0["region_id"] as! Int64
        })
        var locationsByRegion = [Int64: [Location]](minimumCapacity: locationDictsByRegion.count)

        for (regionID, locationDicts) in locationDictsByRegion {
            var locations = [Location]()
            locations.reserveCapacity(locationDicts.count)

            for loc in locationDicts {
                let location = transaction.create(Into<Location>())
                location.latitude = loc["latitude"] as! Double
                location.longitude = loc["longitude"] as! Double
                locations.append(location)
            }

            locationsByRegion[regionID] = locations
        }

        return locationsByRegion
    }

    private func loadRegions(transaction: SynchronousDataTransaction) throws -> [Int64: Region] {
        // Select regions one level at a time, starting with the parents.
        // In this way, we can keep updating our region ID -> Region object
        // mapping, `regionsByID`, and always ensure we have the parents
        // in the dictionary by the time their children need them.
        let regionDicts = try sqlDB.query("""
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
        """)
        var regionsByID = [Int64: Region](minimumCapacity: regionDicts.count)
        let locationsByRegion = try loadLocations(transaction: transaction)

        for r in regionDicts {
            let region = transaction.create(Into<Region>())
            region.name = r["name"] as! String
            let regionID = r["id"] as! Int64
            let parentID = r["parent_id"] as? Int64
            region.regionID = regionID
            region.bounds = locationsByRegion[regionID] ?? []
            region.parent = parentID.flatMap { regionsByID[$0] }
            regionsByID[regionID] = region
        }

        return regionsByID
    }

    private func loadRealEstates(
        transaction: SynchronousDataTransaction,
        users: [Int64: User],
        regions: [Int64: Region]
    ) throws -> [Int64: RealEstate] {
        let realEstateDicts = try sqlDB.query("SELECT * FROM real_estate")
        var realEstatesByID = [Int64: RealEstate](minimumCapacity: realEstateDicts.count)

        for r in realEstateDicts {
            let realEstateID = r["id"] as! Int64
            let ownerID = r["owner_id"] as! Int64
            let regionID = r["region_id"] as! Int64
            let realEstate = transaction.create(Into<RealEstate>())
            realEstate.kind = RealEstateKind(rawValue: r["kind"] as! String)!
            realEstate.owner = users[ownerID]!
            realEstate.region = regions[regionID]!
            realEstatesByID[realEstateID] = realEstate
        }

        return realEstatesByID
    }

    private func loadBookings(
        transaction: SynchronousDataTransaction,
        users: [Int64: User],
        realEstates: [Int64: RealEstate]
    ) throws {
        let bookingDicts = try sqlDB.query("SELECT * FROM booking")

        for b in bookingDicts {
            let userID = b["user_id"] as! Int64
            let realEstateID = b["real_estate_id"] as! Int64
            let startDate = b["start_date"] as! String
            let endDate = b["end_date"] as! String

            let price: NSDecimalNumber
            if let p = b["price"] as? Int64 {
                price = NSDecimalNumber(value: p)
            } else {
                price = NSDecimalNumber(value: b["price"] as! Double)
            }

            let booking = transaction.create(Into<Booking>())
            booking.user = users[userID]!
            booking.realEstate = realEstates[realEstateID]!
            booking.startDate = sqlDB.dateFormatter.date(from: startDate)!
            booking.endDate = sqlDB.dateFormatter.date(from: endDate)!
            booking.category = BookingLengthCategory(startDate: booking.startDate, endDate: booking.endDate)
            booking.price = price
        }
    }
}
