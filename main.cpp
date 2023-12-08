#include <cassert>
#include <iostream>
#include <sstream>
#include "Database.h" 
#include "security.h"

class User : public Serialization::Serializable {
private:
    std::string email;
    std::string password;
    std::pair<long long int, long long int> keys;

public:
    User(const std::string& email, const std::string& password, const std::pair<long long int, long long int>& keys)
        : email(email), password(password), keys(keys) {}

    std::vector<std::string> getContent() const noexcept override {
        return { email, password, std::to_string(keys.first), std::to_string(keys.second) };
    }

    std::string getPrimaryKey() const override {
        return email;
    }
};

class Trip : public Serialization::Serializable {
private:
    int tripId;
    std::string destination;
   

public:
    Trip(int tripId, const std::string& destination)
        : tripId(tripId), destination(destination) {}

    std::vector<std::string> getContent() const noexcept override {
        return { std::to_string(tripId), destination };
    }

    std::string getPrimaryKey() const override {
        return std::to_string(tripId);
    }
};

class Booking : public Serialization::Serializable {
private:
    int bookingId;
    std::string userEmail;
    int tripId;
    

public:
    Booking(int bookingId, const std::string& userEmail, int tripId)
        : bookingId(bookingId), userEmail(userEmail), tripId(tripId) {}

    std::vector<std::string> getContent() const noexcept override {
        return { std::to_string(bookingId), userEmail, std::to_string(tripId) };
    }

    std::string getPrimaryKey() const override {
        return std::to_string(bookingId);
    }
};
class ConsoleApp {
private:
    Auth auth;
    table::Table userTable;  
    table::Table tripsTable;  
    table::Table bookingsTable;  
    std::string currentUser;

public:
    ConsoleApp(table::Table userTable,  
    table::Table tripsTable,  
    table::Table bookingsTable) : auth(Auth(std::unordered_map<std::string, std::pair<std::string, std::pair<long long int, long long int>>>())) , userTable(userTable) , tripsTable(tripsTable) , bookingsTable(bookingsTable) {
    
       
    }


    void run() {
        while (true) {
            std::cout << "1. Login\n2. Register\n3. Exit\n";
            int choice;
            std::cin >> choice;

            switch (choice) {
            case 1:
                handleLogin();
                break;
            case 2:
                handleRegister();
                break;
            case 3:
                std::cout << "Exiting...\n";
                return;
            default:
                std::cout << "Invalid choice. Please try again.\n";
            }
        }
    }

private:
    void handleLogin() {
        std::string email, password;
        std::cout << "Enter email: ";
        std::cin >> email;
        std::cout << "Enter password: ";
        std::cin >> password;

        try {
            if (auth.login(email, password)) {
                currentUser = email;
                showTrips();
                bookTrip();
            }
        }
        catch (const AuthException& e) {
            std::cout << "Authentication failed: " << e.what() << "\n";
        }
    }

    void handleRegister() {
        std::string email, password;
        std::cout << "Enter email: ";
        std::cin >> email;
        std::cout << "Enter password: ";
        std::cin >> password;

        try {
            auto keys = std::make_pair((long long int) 0, (long long int)0);
            if (auth.registerUser(email, password , keys)) {
                std::cout << "Registration successful.\n";
                createUserRecord(User(email , password , keys));
            }
        }
        catch (const AuthException& e) {
            std::cout << "Registration failed: " << e.what() << "\n";
        }
    }

    void createUserRecord(User user) {
        
        std::vector<Serialization::Serializable*> userContent;
        userContent.push_back(&user);
        
        auto userQuery = query::QueryBuilder(query::Type::INSERT).setTarget({ "email", "password" , "publicKey" , "secretKey"}).setPayLoad(std::move(userContent)).build();
        userTable.executeQuery(userQuery);
    }

    void showTrips() {
        // Fetch and display available trips from the 'trips' table
        query::QueryBuilder selectQuery(query::Type::SELECT);
        auto query = selectQuery.setTarget({ "tripId", "destination", "departureDate", "price" }).setPredicate([](const Serialization::Serializable*)->bool {
            return true;
            }).build();
            auto result = tripsTable.executeQuery(query);

            if (result) {
                std::cout << "Available trips:\n";
                for (const auto& trip : *result) {
                    trip->cout();
                }
            }
            else {
                std::cout << "No trips available.\n";
            }
    }

    void bookTrip() {
        int tripId;

        // Prompt the user to enter the trip ID and validate the input
        while (true) {
            std::cout << "Enter the trip ID you want to book: ";
            std::cin >> tripId;

          
            if (isValidTripId(tripId)) {
                break;  // Exit the loop if the input is valid
            }
            else {
                std::cout << "Invalid trip ID. Please try again.\n";
            }
        }

        // Check if the trip exists and is available
        bool isFound = tripExists(tripId);
       
       
        if(isFound) { 
            // Create a new booking record
             Booking newBooking(std::rand(), currentUser.c_str(), tripId);

            // Add the new booking record to the 'bookings' table
            addBooking(newBooking);
            std::cout << "Booking successful!\n";
        }

    }
    bool isValidTripId(int tripId) {
        // Create a query to check if the tripId exists in the 'trips' table
        
        auto query =query::QueryBuilder(query::Type::SELECT).setTarget({ "trip_id" }).setPredicate([tripId](const Serialization::Serializable* obj) -> bool {
            
            return obj->getPrimaryKey() == std::to_string(tripId);
            }).build();

            // Execute the query on the 'trips' table
            auto result = tripsTable.executeQuery(query);

            // Check if any matching record was found
            return result && !result->empty();
    }
    void addBooking( Booking& booking) {
        // Create a query to insert the booking into the 'bookings' table
        query::QueryBuilder insertQuery(query::Type::INSERT);
        auto query = insertQuery.setTarget({ "bookingId", "userEmail", "tripId" }).setPayLoad(std::vector<Serialization::Serializable*>{ &booking }).build();

        // Execute the query on the 'bookings' table
        bookingsTable.executeQuery(query);
    }
    bool tripExists(int tripId) {
        // Create a query to check if a trip with the given tripId exists
        
        auto query = query::QueryBuilder(query::Type::SELECT).setTarget({ "tripId" }).setPredicate([tripId](const Serialization::Serializable* obj) -> bool {
            const auto* trip = dynamic_cast<const Trip*>(obj); // Assuming Trip is the class representing a trip
            return trip && std::stoi(trip->getPrimaryKey()) == tripId;
            }).build();

            // Execute the query on the 'trips' table
            auto result = tripsTable.executeQuery(query);

            // Check if the result contains any entries
            return result && !result->empty();
    }


};


int main() {
   
    // Initialize the trips table
    fileIO::FileStream tripsFileStream("trips.csv", "Trips");
    Serialization::Deserializer tripsDeserializer;
    Serialization::Serializer tripsSerializer;
    Serialization::FormatDescriptor tripsFormatDescriptor;
    table::Cursor tripsCursor(tripsFileStream, tripsDeserializer, tripsSerializer, tripsFormatDescriptor);
    auto tripsTable = table::Table(tripsCursor, std::vector<std::string>{ "tripId", "destination", "departureDate", "price" }, "trips.csv");

    // Initialize the bookings table
    fileIO::FileStream bookingsFileStream("bookings.csv", "Bookings");
    Serialization::Deserializer bookingsDeserializer;
    Serialization::Serializer bookingsSerializer;
    Serialization::FormatDescriptor bookingsFormatDescriptor;
    table::Cursor bookingsCursor(bookingsFileStream, bookingsDeserializer, bookingsSerializer, bookingsFormatDescriptor);
    auto bookingsTable = table::Table(bookingsCursor, std::vector<std::string>{ "bookingId", "userEmail", "tripId" }, "bookings.csv");

    // Initialize the user table
    fileIO::FileStream usersFileStream("users.csv", "Users");
    Serialization::Deserializer usersDeserializer;
    Serialization::Serializer usersSerializer;
    Serialization::FormatDescriptor usersFormatDescriptor;
    table::Cursor usersCursor(usersFileStream, usersDeserializer, usersSerializer, usersFormatDescriptor);
    auto userTable = table::Table(usersCursor, std::vector<std::string>{ "userEmail", "password", "publicKey", "privateKey" }, "users.csv");
    // Create the console application
    ConsoleApp app(userTable , tripsTable , bookingsTable);

    // Run the application
    app.run();

    return 0;
}
