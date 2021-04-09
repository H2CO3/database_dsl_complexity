import Foundation
import CoreData
import CoreStore

final class Queries {
    private let dataStack: DataStack

    // helper for wrapping query functions
    private func w<T: Encodable>(_ fn: @escaping ([Any?]) throws -> [T]) -> ([Any?]) throws -> [[String: Any]] {
        return { params in
            let encoder = JSONEncoder()
            let results = try fn(params)

            // This is super inefficient, but we don't care,
            // as we are only converting small amounts of data.
            return try JSONSerialization.jsonObject(
                with: try encoder.encode(results)
            ) as! [[String: Any]]
        }
    }

    var queries: [String: ([Any?]) throws -> [[String: Any]]] { [
        "continents": w(queryContinents),
        "siblings_and_parents": w(self.querySiblingsAndParents),
        "no_login_users": w(queryNoLoginUsers),
        "num_valid_sessions": w(queryNumValidSessions),
        "multi_profile_users": w(queryMultiProfileUsers),
        "owned_real_estate_region_count": w(queryOwnedRealEstateRegionCount),
        "profile_counts_by_non_google_user": w(queryProfileCountsByNonGoogleUser),
        "avg_daily_price_by_user_by_booking_length_category": w(
            queryAvgDailyPriceByUserByBookingLengthCategory
        ),
        "top_n_booked_regions_for_user_x": w(queryTopNBookedRegionsForUserX),
        "northest_booked_latitude_slow": w(queryNorthestBookedLatitudeSlow),
        "northest_booked_latitude_fast": w(queryNorthestBookedLatitudeFast),
    ] }

    init(dataStack: DataStack) {
        self.dataStack = dataStack
    }

    // Queries and their strongly-typed return types.
    //
    // Static return types are exposed (as opposed to
    // just making every function return [[String: Any]]
    // directly) for demonstrating the type-safety
    // capabilities of CoreStore.

    struct RegionResult: Codable {
        let id: Int64
        @NullCodable
        var parentID: Int64?
        let name: String

        enum CodingKeys: String, CodingKey {
            case id = "id"
            case parentID = "parent_id"
            case name = "name"
        }
    }

    func queryContinents(params: [Any?]) throws -> [RegionResult] {
        let rows = try dataStack.fetchAll(
            From<Region>()
                .where(\.$parent == nil)
                .orderBy(.ascending(\.$regionID))
        )
        // This can't be solved directly using `queryAttributes()`,
        // and in a type-safe way, due to the nested property access
        // (`parent?.regionID`) through an optional.
        // It would work with the string-based API, though.
        return rows.map {
            RegionResult(
                id: $0.regionID,
                parentID: $0.parent?.regionID,
                name: $0.name
            )
        }
    }

    func querySiblingsAndParents(params: [Any?]) throws -> [RegionResult] {
        let childID = params[0] as! Int64

        // Core Data, `NSFetchRequest`, and `NSPredicate` apparently
        // do not support recursive queries. Most of the advice out
        // there consists of just doing the graph traversal from
        // the host code (i.e. Swift or Objective-C), which does
        // entail issuing multiple queries, but some of the memory
        // costs are mitigated by faulting.
        //
        // However, we need to wrap this in a transaction,
        // exactly because we are fetching in a loop.
        //
        // Also order each returned level by the second
        // overall sort column, i.e. region ID, because in
        // this manner, we will only have to sort on the
        // parent ID during the in-memory postprocessing.
        return try dataStack.perform(synchronous: { transaction in
            var results: [Region] = []

            let seed = try transaction.fetchOne(
                From<Region>()
                    .where(\.$regionID == childID)
            )
            var parent = seed?.parent

            while let region = parent {
                results += region.children // add siblings
                parent = region.parent // climb up one level
            }

            // Add root regions, i.e. the ones with a nil parent
            let roots = try transaction.fetchAll(
                From<Region>()
                    .where(\.$parent == nil)
            )
            results += roots

            // There is no region with ID 0, intentionally,
            // so that we can treat 0 as the nil value.
            results.sort {
                let p0 = $0.parent?.regionID ?? 0
                let p1 = $1.parent?.regionID ?? 0
                return p0 == p1 ? $0.regionID < $1.regionID : p0 > p1
            }

            return results.map {
                RegionResult(
                    id: $0.regionID,
                    parentID: $0.parent?.regionID,
                    name: $0.name
                )
            }
        })
    }

    struct NoLoginResult: Codable {
        let userID: Int64

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
        }
    }

    func queryNoLoginUsers(params: [Any?]) throws -> [NoLoginResult] {
        let rows = try dataStack.queryAttributes(
            From<User>()
                .select(NSDictionary.self, .attribute(\.$userID))
                .where(format: "sessions.@count == %@", 0)
                .orderBy(.ascending(\.$userID))
        )
        return rows.map {
            NoLoginResult(userID: $0["userID"] as! Int64)
        }
    }

    struct ValidSessionResult: Codable {
        let userID: Int64
        let validSessions: Int

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
            case validSessions = "valid_sessions"
        }
    }

    func queryNumValidSessions(params: [Any?]) throws -> [ValidSessionResult] {
        let users = try dataStack.fetchAll(
            From<User>().orderBy(.ascending(\.$userID))
        )

        return users.map { u in
            ValidSessionResult(
                userID: u.userID,
                validSessions: u.sessions.filter({ $0.logout == nil }).count
            )
        }
    }

    struct ProfileCountResult: Codable {
        let userID: Int64
        let profileCount: Int64

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
            case profileCount = "profile_count"
        }
    }

    func queryMultiProfileUsers(params: [Any?]) throws -> [ProfileCountResult] {
        let rows = try dataStack.queryAttributes(
            From<User>()
                .select(NSDictionary.self,
                       .attribute(\.$userID))
                       // .count(\.$profiles, as: "...") does not compile;
                       // .count("profiles", as: "...") returns the wrong data
                .where(format: "profiles.@count > %@", 1)
                .orderBy(.ascending(\.$userID))
                .tweak { fetchRequest in // therefore we need this `tweak`
                    let expr = NSExpression(forKeyPath: "profiles.@count")
                    let desc = NSExpressionDescription()
                    desc.expression = expr
                    desc.name = "profileCount"
                    desc.expressionResultType = .integer64AttributeType
                    fetchRequest.propertiesToFetch?.append(desc)
                }
        ) as! [[String: Int64]]

        return rows.map {
            ProfileCountResult(
                userID: $0["userID"]!,
                profileCount: $0["profileCount"]!
            )
        }
    }

    struct RegionCountResult: Codable {
        let userID: Int64
        let regionCount: Int

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
            case regionCount = "region_count"
        }
    }

    func queryOwnedRealEstateRegionCount(params: [Any?]) throws -> [RegionCountResult] {
        // It is spectacularly tricky to get even a non-distinct
        // count out of Core Data, if it's not the top-level
        // count of all objects (which can be obtained via `fetchCount`),
        // or the count of a scalar property immediately inside the
        // entity that is being grouped by another immediate scalar
        // property (which could be obtained by a simple `.groupBy`
        // and a `.select(.count("kind"))`, for example).
        //
        // The latter would be good enough if we wanted to count
        // the owned real estates for a given user; however, we
        // want to traverse the 'region' relationship as well,
        // in order to count the unique region IDs.
        //
        // This does not seem to be possible, because KVC collection
        // operators apparently can't be used for projection, and
        // according to the official documentation, aggregation and
        // set operators are not supported by Core Data -- see here:
        // https://developer.apple.com/documentation/foundation/nsexpression
        //
        // So even `.tweak()`ing the raw `NSFetchRequest` with an extra
        // `@distinctUnionOfObjects.region.regionID` isn't allowed.
        //
        // Below are two of several failed attempts based on the
        // ideas described above.

        // Attempt #1: (only includes users with at least 1 owned real estate)
        // let rows = try dataStack.queryAttributes(
        //     From<RealEstate>()
        //         .select(NSDictionary.self,
        //         .attribute("owner.userID"))
        //         .groupBy("owner.userID")
        //         .orderBy(.ascending("owner.userID"))
        //         .tweak { fetchRequest in
        //             let expr = NSExpression(forKeyPath: "@distinctUnionOfObjects.region.regionID")
        //             let desc = NSExpressionDescription()
        //             desc.name = "ownedRegionIDs"
        //             desc.expression = expr
        //             desc.expressionResultType = .undefinedAttributeType
        //             fetchRequest.propertiesToFetch?.append(desc)
        //         }
        // )

        // Attempt #2:
        // let rows = try dataStack.queryAttributes(
        //     From<User>()
        //         .select(NSDictionary.self, .attribute(\.$userID))
        //         .orderBy(.ascending(\.$userID))
        //         .tweak { fetchRequest in
        //             let expr = NSExpression(format: """
        //             (%@) UNION (
        //                 SUBQUERY(
        //                     ownedRealEstates.region.regionID,
        //                     $rid,
        //                     0 == 0
        //                 )
        //             ).@count
        //             """, argumentArray: [NSSet()])
        //             let desc = NSExpressionDescription()
        //             desc.name = "ownedRegionCount"
        //             desc.expression = expr
        //             desc.expressionResultType = .integer64AttributeType
        //             fetchRequest.propertiesToFetch?.append(desc)
        //         }
        // )

        // So I once again ended up doing it using in-memory
        // aggregation, a GROUP BY on both columns (user ID
        // and region ID), and, in a separate statement,
        // unioning the results with all user IDs.
        return try dataStack.perform(synchronous: { transaction -> [RegionCountResult] in
            let userIDs = try transaction.queryAttributes(
                From<User>()
                    .select(NSDictionary.self, .attribute(\.$userID))
                    .orderBy(.ascending(\.$userID))
            ) as! [[String: Int64]]

            let usersAndRegions = try transaction.queryAttributes(
                From<RealEstate>()
                    .select(NSDictionary.self,
                            .attribute("owner.userID"),
                            .attribute("region.regionID"))
                    .groupBy("owner.userID", "region.regionID")
            ) as! [[String: Int64]]
            let regionsByUsers = Dictionary(grouping: usersAndRegions, by: {
                $0["owner.userID"]!
            })

            return userIDs.map {
                let userID = $0["userID"]!

                return RegionCountResult(
                    userID: userID,
                    regionCount: regionsByUsers[userID]?.count ?? 0
                )
            }
        })
    }

    struct NonGoogleUserResult: Codable {
        let userID: Int64
        let fbProfileCount: Int
        let internalProfileCount: Int

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
            case fbProfileCount = "fb_profile_count"
            case internalProfileCount = "internal_profile_count"
        }
    }

    func queryProfileCountsByNonGoogleUser(params: [Any?]) throws -> [NonGoogleUserResult] {
        let entity = dataStack.entityDescription(for: GoogleProfile.self)!

        // It does not seem to be possible to do this in a single query,
        // because the only place filtering for a type (`entity == %@`)
        // works is inside the top-level `where` clause, for some reason.
        //
        // Thus, we first fetch the user IDs we are interested in, then
        // we manually count the Facebook and Internal profiles in the
        // relevant users, themselves fethched by ID.
        //
        // But in order to avoid inconsistencies between the two queries,
        // we must wrap them in a transaction.
        //
        // An alternative solution would be to store different kinds of
        // profiles in different properties (`User.facebookProfiles`,
        // `User.googleProfiles`, etc.), and then we could access their
        // `@count` directly. However, this defeats the purpose of
        // inheritance, and it would complicate queries that need all
        // three (or more) subtypes of `Profile` at the same time.
        return try dataStack.perform(synchronous: { transaction -> [NonGoogleUserResult] in
            // We can't use `fetchAll()`, we need `queryAttributes()`,
            // because only this function works with GROUP BY.
            // (Also, `fetchAll()` would return `Profile`s, not `User`s.)
            let userIDRows = try transaction.queryAttributes(
                From<Profile>(),
                Select<Profile, NSDictionary>(.attribute("user.userID")),
                Where<Profile>("entity == %@", entity),
                GroupBy<Profile>("user.userID")
            )

            // Technically, in a realistic scenario, the performance
            // of this would be terrible, because we need to retrieve
            // all the user IDs in the _complementer_ set, which is
            // probably huge, given that the result set is usually small.
            //
            // However, grouping by the positives does not work, since
            // then we would get all users that have a profile different
            // from a Google profile, and not those that do not have any
            // Google profiles.
            //
            // The other alternative would be to count the number of
            // facebook, google, and internal profiles, just like we
            // would do it in SQL. However, CoreData's NSPredicate
            // handling doesn't seem to allow that, at least not in
            // a reasonably straightforward way that I was able to
            // discover after performing extensive research of the API.
            let userIDs = userIDRows.map { $0["user.userID"] as! Int64 }
            let users = try transaction.fetchAll(
                From<User>()
                    .where(!(userIDs ~= \.$userID))
                    .orderBy(.ascending(\.$userID))
            )

            // We must perform the mapping _inside_ the transaction,
            // because it seems to be invalid to return an object
            // to outside the transaction closure. Doing so results
            // in mysterious crashes related to unwrapping optionals
            // and empty collection (to-many relationship) properties.
            return users.map { u in
                NonGoogleUserResult(
                    userID: u.userID,
                    fbProfileCount: u.profiles.filter({ $0 is FacebookProfile }).count,
                    internalProfileCount: u.profiles.filter({ $0 is InternalProfile }).count
                )
            }
        })
    }

    struct AvgDailyPriceResult: Codable {
        let username: String
        let duration: BookingLengthCategory
        @NullCodable
        var averageDailyPrice: Double?

        enum CodingKeys: String, CodingKey {
            case username = "username"
            case duration = "duration"
            case averageDailyPrice = "avg_daily_price"
        }
    }

    func queryAvgDailyPriceByUserByBookingLengthCategory(params: [Any?]) throws -> [AvgDailyPriceResult] {
        // It does not seem to be possible to perform scalar operations
        // on projected fields using `NSExpression` in `.tweak`, but
        // this is not a problem, because we can just sum the start and
        // end dates, as `sum(ends - starts) == sum(ends) - sum(starts)`.
        // Then performing the `sum(price) / sum(durations)` projection
        // does not pull more rows into memory than necessary, because
        // we need all the returned (user, category) pairs anyway.
        //
        // We still do need to perform one additional query, in order
        // to pull all usernames. Therefore, we perform a transaction.
        return try dataStack.perform(synchronous: { transaction in
            let rows = try transaction.queryAttributes(
                From<Booking>()
                    .select(NSDictionary.self,
                            .attribute("user.username"),
                            .attribute(\.$category),
                            .sum(\.$startDate, as: "sumStartDate"),
                            .sum(\.$endDate, as: "sumEndDate"),
                            .sum(\.$price, as: "price"))
                    .groupBy("user.username", "category")
                    // .orderBy(.ascending("user.username"),
                    //          .ascending("category"))
            )
            // this does not actually load all the data thanks to faulting
            let users = try transaction.fetchAll(
                From<User>()
                    .orderBy(.ascending(\.$username))
            )

            // It would be cleaner to use a dictionary from (username, category)
            // here, but unfortunately, tuples can't be Hashable.
            // Consequently, we are forced to build a nested data structure.
            let rowsByUser = Dictionary(grouping: rows, by: {
                $0["user.username"] as! String
            })
            let priceArr = rowsByUser.map { (username, rows) -> (String, [BookingLengthCategory: Double]) in
                let priceByCategory = Dictionary(uniqueKeysWithValues: rows.map { row -> (BookingLengthCategory, Double) in
                    let category = BookingLengthCategory(
                        rawValue: row["category"] as! String
                    )!
                    let price = row["price"] as! Double
                    let sumStartDate = row["sumStartDate"] as! Double
                    let sumEndDate = row["sumEndDate"] as! Double
                    let days = (sumEndDate - sumStartDate) / (24 * 60 * 60)

                    return (category, price / days)
                })
                return (username, priceByCategory)
            }
            let priceDict = Dictionary(uniqueKeysWithValues: priceArr)

            let categories = BookingLengthCategory.allCases.sorted {
                $0.rawValue < $1.rawValue
            }

            // Iterate over Cartesian product of all users and all categories
            // in order to emulate the LEFT JOIN in-memory.
            return users.flatMap { u in
                categories.map { c in
                    AvgDailyPriceResult(
                        username: u.username,
                        duration: c,
                        averageDailyPrice: priceDict[u.username]?[c]
                    )
                }
            }
        })
    }

    struct TopRegionResult: Codable {
        let regionName: String
        let frequency: Int

        enum CodingKeys: String, CodingKey {
            case regionName = "region_name"
            case frequency = "frequency"
        }
    }

    func queryTopNBookedRegionsForUserX(params: [Any?]) throws -> [TopRegionResult] {
        let userID = params[0] as! Int64
        let limit = params[1] as! Int

        // It looks like Core Data does not allow sorting
        // based on the result of aggregate functions (or
        // any non-attribute expression); it wants to sort
        // using actual, stored properties of an entity.
        // See more: https://stackoverflow.com/a/14794834
        var rows = try dataStack.queryAttributes(
            From<Booking>()
                .select(NSDictionary.self,
                    .attribute("realEstate.region.name"),
                    // doesn't matter which attribute as long as it's a scalar
                    .count(\.$price, as: "frequency"))
                .where(\.$user ~ \.$userID == userID)
                .groupBy("realEstate.region.name")
        )

        // There appears to be no standard partial sort in Swift, either
        rows.sort {
            let f0 = $0["frequency"] as! Int
            let f1 = $1["frequency"] as! Int
            let n0 = $0["realEstate.region.name"] as! String
            let n1 = $1["realEstate.region.name"] as! String
            return f0 == f1 ? n0 < n1 : f0 > f1
        }

        let endIdx = min(limit, rows.count)

        return rows[..<endIdx].map {
            TopRegionResult(
                regionName: $0["realEstate.region.name"] as! String,
                frequency: $0["frequency"] as! Int
            )
        }
    }

    struct NorthestLatitudeResult: Codable {
        let userID: Int64

        @NullCodable
        var northestLatitude: Double?

        enum CodingKeys: String, CodingKey {
            case userID = "user_id"
            case northestLatitude = "northest_latitude"
        }
    }

    func queryNorthestBookedLatitudeSlow(params: [Any?]) throws -> [NorthestLatitudeResult] {
        // The naive approach, `.maximum("bookings.realEstate.region.bounds.latitude")`,
        // does not work, nor does `_.@max` in `.tweak()`.
        // So, once again, we resort to aggregating the Bookings,
        // then emulating the left join by retrieving all users
        // in a separate query in the same transaction.
        return try dataStack.perform(synchronous: { transaction in
            let rows = try transaction.queryAttributes(
                From<Booking>()
                    .select(NSDictionary.self,
                            .attribute("user.userID"),
                            .maximum("realEstate.region.bounds.latitude", as: "northestLatitude"))
                    .groupBy("user.userID")
            )
            let dicts = Dictionary(uniqueKeysWithValues: rows.map {
                ($0["user.userID"] as! Int64, $0["northestLatitude"] as? Double)
            })

            let users = try transaction.fetchAll(
                From<User>()
                    .orderBy(.ascending(\.$userID))
            )

            // Emulate LEFT JOIN
            return users.map {
                NorthestLatitudeResult(
                    userID: $0.userID,
                    northestLatitude: dicts[$0.userID] ?? nil
                )
            }
        })
    }

    func queryNorthestBookedLatitudeFast(params: [Any?]) throws -> [NorthestLatitudeResult] {
        // This is the same approach as we followed in all other
        // examples: we pre-compute the maximal latitude of each
        // region. However, we need to do this in code, due to the
        // lack of ability to fetch from temporary collections.
        return try dataStack.perform(synchronous: { transaction in
            let rows = try transaction.queryAttributes(
                From<Region>()
                    .select(NSDictionary.self,
                            .attribute(\.$regionID),
                            .maximum("bounds.latitude", as: "maxLatitude"))
            )

            let dicts = Dictionary(uniqueKeysWithValues: rows.map {
                ($0["regionID"] as! Int64, $0["maxLatitude"] as! Double)
            })

            // Emulate LEFT JOIN
            let users = try transaction.fetchAll(
                From<User>()
                    .orderBy(.ascending(\.$userID))
            )

            return users.map { u in
                let maxLatitudes = u.bookings.compactMap {
                    dicts[$0.realEstate.region.regionID]
                }

                return NorthestLatitudeResult(
                    userID: u.userID,
                    northestLatitude: maxLatitudes.max()
                )
            }
        })
    }
}

/// This is extremely stupid. Auto-generated `Codable` impls
/// omit the encoding of optional values in dictionaries,
/// and there are no knobs to turn in order to change this
/// behavior. A frequently suggested solution is "just write
/// your own Codable implementation", but that misses the
/// whole point of Codable: easy auto-generated code free of
/// boilerplate and related errors.
///
/// This alternative solution was suggested on Stack Overflow:
/// https://stackoverflow.com/a/62312021
///
/// It still requires manually marking all optional properties
/// as `@NullCodable`, but at least that minimizes the
/// amount of boilerplate needed to make this work.
@propertyWrapper
struct NullCodable<T> {
    var wrappedValue: T?

    init(wrappedValue: T?) {
        self.wrappedValue = wrappedValue
    }
}

extension NullCodable: Encodable where T: Encodable {
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch wrappedValue {
        case .some(let value): try container.encode(value)
        case .none: try container.encodeNil()
        }
    }
}

extension NullCodable: Decodable where T: Decodable {
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            wrappedValue = .none
        } else {
            let value = try container.decode(T.self)
            wrappedValue = .some(value)
        }
    }
}
