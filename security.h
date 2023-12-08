#pragma once
#include <random>
#include <vector>
#include <string>
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <cmath>
#include <unordered_map>
class RSAManager {
private:
    long long int n;

    long long int generatePrime(long long int p, long long int q) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<long long int> distribution(p, q);

        long long int random_number;
        do {
            random_number = distribution(gen);
        } while (!isPrime(random_number));

        return random_number;
    }

    bool isPrime(long long int num) {
        if (num <= 1) return false;
        for (long long int i = 2; i <= std::sqrt(num); i++) {
            if (num % i == 0) return false;
        }
        return true;
    }

    long long int calculateCmmdc(long long int a, long long int b) {
        long long int r;
        while (b != 0) {
            r = a % b;
            a = b;
            b = r;
        }
        return a;
    }

    std::pair<long long int, long long int> calculateKeys() {
        long long int p = generatePrime(1, 100);
        long long int q = generatePrime(1, 100);
        n = p * q;
        int e = findEulerTotient(p, q);
        long long int phi = (p - 1) * (q - 1);
        int d = calculateD(e, phi);
        return { e, d };
    }

    int findEulerTotient(long long int p, long long int q) {
        int e = 2;
        while (calculateCmmdc(e, (p - 1) * (q - 1)) != 1) {
            e++;
        }
        return e;
    }

    int calculateD(int e, long long int phi) {
        int d = 2;
        while ((d * e) % phi != 1) {
            d++;
        }
        return d;
    }

    long long int applyExponentiation(long long int base, int exponent, long long int mod) {
        long long int result = 1;
        while (exponent > 0) {
            if (exponent % 2 == 1) {
                result = (result * base) % mod;
            }
            exponent = exponent >> 1;
            base = (base * base) % mod;
        }
        return result;
    }

public:
    std::pair<long long int, long long int> createKeys() {
        return calculateKeys();
    }

    long long int encrypt(long long int message, int public_key) {
        long long int e = public_key;
        return applyExponentiation(message, e, n);
    }

    long long int decrypt(long long int encrypted_message, int private_key) {
        int d = private_key;
        return applyExponentiation(encrypted_message, d, n);
    }

    std::vector<long long int> encode(const std::string& password, int public_key) {
        std::vector<long long int> encrypted_passwords;
        for (char character : password) {
            encrypted_passwords.push_back(encrypt(character, public_key));
        }
        return encrypted_passwords;
    }

    std::string decode(const std::vector<long long int>& encrypted_passwords, int private_key) {
        std::string password;
        for (long long int encrypted_char : encrypted_passwords) {
            password += static_cast<char>(decrypt(encrypted_char, private_key));
        }
        return password;
    }
};

class AuthException : public std::exception {
public:
    explicit AuthException(const char* message) : std::exception(message) {}
};

class ExistingUserException : public AuthException {
public:
    explicit ExistingUserException() : AuthException("User with the provided email already exists.") {}
};

class InvalidEmailException : public AuthException {
public:
    explicit InvalidEmailException() : AuthException("Invalid email format.") {}
};

class InvalidCredentialsException : public AuthException {
public:
    explicit InvalidCredentialsException() : AuthException("Invalid email or password.") {}
};
class Auth {
private:
    std::unordered_map<std::string, std::pair<std::string, std::pair<long long int, long long int>>> users;
    RSAManager cryptManager;

public:
    Auth(const std::unordered_map<std::string, std::pair<std::string, std::pair<long long int, long long int>>>& userData)
        : users(userData) {}

    bool registerUser(const std::string& email, const std::string& password , std::pair<long long int, long long int>& secretKeys) {
        // Check for valid email format
        if (email.find('@') == std::string::npos || email.find('.') == std::string::npos) {
            throw InvalidEmailException();
        }

        // Check if the user already exists
        if (users.find(email) != users.end()) {
            throw ExistingUserException();
        }

        auto keys = cryptManager.createKeys();
        std::vector<long long int> encryptedPassword = cryptManager.encode(password, keys.first);
        users[email] = { password, keys };
        secretKeys = keys;
        return true;
    }

    bool login(const std::string& email, const std::string& password) {
        auto it = users.find(email);
        if (it != users.end()) {
            const auto& [storedPassword, keys] = it->second;
            std::vector<long long int> encryptedPassword = cryptManager.encode(password, keys.first);
            if (encryptedPassword == cryptManager.encode(storedPassword, keys.first)) {
                return true;
            }
            else {
                throw InvalidCredentialsException();
            }
        }
        throw InvalidCredentialsException();
    }
};
