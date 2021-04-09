import Foundation
import CoreData
import CoreStore

final class User: CoreStoreObject {
    // This is only here because CoreStore does not seem
    // to allow specifying the type or the value of the
    // unique ID / primary key.
    @Field.Stored("userID")
    var userID: Int64 = 0

    @Field.Stored("username")
    var username: String = ""

    @Field.Stored("realName")
    var realName: String?

    @Field.Stored("birthDate")
    var birthDate: Date?

    @Field.Relationship("profiles", inverse: \.$user)
    var profiles: [Profile]

    @Field.Relationship("sessions", inverse: \.$user)
    var sessions: [Session]

    @Field.Relationship("ownedRealEstates")
    var ownedRealEstates: [RealEstate]

    @Field.Relationship("bookings")
    var bookings: [Booking]
}

class Profile: CoreStoreObject {
    @Field.Relationship("user")
    var user: User!
}

final class FacebookProfile: Profile {
    @Field.Stored("accountID")
    var accountID: String = ""
}

final class GoogleProfile: Profile {
    @Field.Stored("accountID")
    var accountID: String = ""

    @Field.Stored("email")
    var email: String?

    @Field.Stored("imageURL")
    var imageURL: URL?
}

final class InternalProfile: Profile {
    @Field.Stored("passwordHash")
    var passwordHash: Data = Data()

    @Field.Stored("passwordSalt")
    var passwordSalt: Data = Data()
}

final class Session: CoreStoreObject {
    @Field.Relationship("login")
    var login: AuthEvent!

    @Field.Relationship("logout")
    var logout: AuthEvent?

    @Field.Relationship("user")
    var user: User!
}

final class AuthEvent: CoreStoreObject {
    @Field.Stored("publicKey")
    var publicKey: Data = Data()

    @Field.Stored("date")
    var date: Date = Date()
}

enum RealEstateKind: String, Equatable, ImportableAttributeType, FieldStorableType {
    case apartment
    case house
    case mansion
    case penthouse
}

final class RealEstate: CoreStoreObject {
    @Field.Stored("kind")
    var kind: RealEstateKind!

    @Field.Relationship("owner", inverse: \.$ownedRealEstates)
    var owner: User!

    @Field.Relationship("region", inverse: \.$realEstates)
    var region: Region!

    @Field.Relationship("bookings")
    var bookings: [Booking]
}

final class Region: CoreStoreObject {
    // This is only here because CoreStore does not seem
    // to allow specifying the type or the value of the
    // unique ID / primary key.
    @Field.Stored("regionID")
    var regionID: Int64 = 0

    @Field.Stored("name")
    var name: String = ""

    @Field.Relationship("parent", inverse: \.$children)
    var parent: Region?

    @Field.Relationship("children")
    var children: [Region]

    @Field.Relationship("bounds", inverse: \.$region)
    var bounds: [Location]

    @Field.Relationship("realEstates")
    var realEstates: [RealEstate]
}

final class Location: CoreStoreObject {
    @Field.Stored("latitude")
    var latitude: Double = 0.0

    @Field.Stored("longitude")
    var longitude: Double = 0.0

    @Field.Relationship("region")
    var region: Region!
}

final class Booking: CoreStoreObject {
    @Field.Stored("startDate")
    var startDate: Date = Date()

    @Field.Stored("endDate")
    var endDate: Date = Date()

    // This property is redundant: it can be (and it in fact _is_)
    // computed from `startDate` and `endDate`. However, we still
    // need it so that we can group by it, which is not possible
    // using plain computed properties, because they are not run
    // by the database storage layer.
    @Field.Stored("category")
    var category: BookingLengthCategory! {
        willSet {
            if category != nil {
                fatalError("Not allowed to re-set `Booking.category`")
            }
        }
    }

    @Field.Stored("price")
    var price: NSDecimalNumber!

    @Field.Relationship("user", inverse: \.$bookings)
    var user: User!

    @Field.Relationship("realEstate", inverse: \.$bookings)
    var realEstate: RealEstate!
}

enum BookingLengthCategory: String, Equatable, CaseIterable, Codable, FieldStorableType, ImportableAttributeType {
    case days
    case weeks
    case months
    case years
    case centuries
}

extension BookingLengthCategory {
    init(startDate: Date, endDate: Date) {
        self.init(duration: endDate.timeIntervalSince(startDate))
    }

    init(duration: TimeInterval) {
        let days = duration / (24 * 60 * 60)

        if days >= 36525 {
            self = .centuries
        } else if days >= 365 {
            self = .years
        } else if days >= 30 {
            self = .months
        } else if days >= 7 {
            self = .weeks
        } else {
            self = .days
        }
    }
}

////////////////

func makeDataStack(at path: String) throws -> DataStack {
    let dataStack = DataStack(
        CoreStoreSchema(
            modelVersion: "V1",
            entities: [
                Entity<User>("User"),
                Entity<Profile>("Profile", isAbstract: true),
                Entity<FacebookProfile>("FacebookProfile"),
                Entity<GoogleProfile>("GoogleProfile"),
                Entity<InternalProfile>("InternalProfile"),
                Entity<Session>("Session"),
                Entity<AuthEvent>("AuthEvent"),
                Entity<RealEstate>("RealEstate"),
                Entity<Region>("Region"),
                Entity<Location>("Location"),
                Entity<Booking>("Bounds"),
            ]
        )
    )

    try dataStack.addStorageAndWait(
        SQLiteStore(fileURL: URL(fileURLWithPath: path))
    )

    // Since modifying the entity hierarchy drops indexes,
    // we need to make sure that adding indexes is the
    // last step in setting up the data stack.
    // addIndexes(to: dataStack)

    return dataStack
}

private func addIndexes(to dataStack: DataStack) {
    let user = dataStack.entityDescription(for: User.self)!
    let usernameIndex = index(for: .ascending("username"))
    let birthDateIndex = index(for: .ascending("birthDate"))
    user.indexes.append(usernameIndex)
    user.indexes.append(birthDateIndex)

    let authEvent = dataStack.entityDescription(for: AuthEvent.self)!
    let pubkeyIndex = index(for: .ascending("publicKey"))
    authEvent.indexes.append(pubkeyIndex)
}

private enum IndexComponent {
    case ascending(String)
    case descending(String)

    var name: String {
        switch self {
            case .ascending(let s): return s
            case .descending(let s): return s
        }
    }

    var isAscending: Bool {
        switch self {
            case .ascending: return true
            case .descending: return false
        }
    }
}

private func index(for components: IndexComponent...) -> NSFetchIndexDescription {
    let name = "index_" + components.map({ $0.name }).joined(separator: "_")
    let elements = components.map { prop -> NSFetchIndexElementDescription in
        let desc = NSFetchIndexElementDescription(
            property: {
                let desc = NSAttributeDescription()
                desc.name = prop.name
                return desc
            }(),
            collationType: .binary
        )
        desc.isAscending = prop.isAscending
        return desc
    }

    return NSFetchIndexDescription(name: name, elements: elements)
}
