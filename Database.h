#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <ranges>
#include <fstream>
#include <sstream>
#include <functional>
#include <optional>
namespace fileIO {

	class FileStream{
	private:
		const char* m_completePath;
		const char* tag;
		std::fstream fileStream;
		
	public:
		FileStream(const char* path , const char * tag);
		~FileStream()noexcept;
		FileStream& operator=(const FileStream& other);
		
		FileStream(const FileStream& other);
		
		bool deleteLine(size_t lineNumber , const char * delim);
		bool overrideLine(size_t lineNumber, const char* newContent);
		void reopen();
		bool is_open() {
			return fileStream.is_open();
		}
		const char* getPath() {
			return m_completePath;
		}
		bool is_bad() {
			return fileStream.bad();
		}
		void moveCarreteToEnd()noexcept;
		void moveCarreteToLine(size_t where);
		void moveCarreteToBegin()noexcept;
		void putLine(const char* formattedLine) noexcept;
		const char* getTag() const noexcept;
		const char * getNextLine(std::string& dest ,const char* delim)noexcept;
		std::string getFileContent()  noexcept;

		
	};

	

	


}

namespace Serialization {

	struct Serializable {
	
		virtual std::vector<std::string> getContent() const noexcept = 0;
		virtual std::string getPrimaryKey() const = 0;
		virtual ~Serializable() = default;
		void cout() const {
			std::cout << "Row = ";
			for (auto item : getContent()) {
				std::cout << item << " ";
			}
			std::cout << "\n";
		}
	};

	class RowEntry : public Serializable {
	private:
		std::vector<std::string> m_content;
	public:
		RowEntry(std::vector<std::string>& rows) : Serializable()  , m_content(rows){}
		std::vector<std::string> getContent() const noexcept override {
			return m_content;
		}
		
		std::string getPrimaryKey() const override {
			return getContent().at(0);
		}
	};


	struct FormatDescriptor {
		virtual const char* getColumnSeparator() const noexcept {
			return ",";
		}
		virtual const char* getRowSeparator() const noexcept {
			return "\n";
		}
		virtual const char* getColumnSeparatorSubstitute() const noexcept {
			return "<'>";
		}
		virtual const char* getRowSeparatorSubstitute() const noexcept {
			return "<|>";
		}
	
	};

	struct Serializer {
	private:
		
	public:
		virtual  std::string serialize(const Serializable* obj, FormatDescriptor* fd);
		void sanitizeField( std::string& field ,FormatDescriptor* fd);
	};
    
	struct Deserializer {
	private:
		

	public:
		void removeSanitation( std::string& field , FormatDescriptor* fd);
		virtual std::vector<std::string> deserialize(const char* line, FormatDescriptor* fd);
	};

}
namespace util {
	
	inline std::string ReplaceAll(std::string str, const std::string& from, const std::string& to) {
		size_t start_pos = 0;
		while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
			str.replace(start_pos, from.length(), to);
			start_pos += to.length(); // Handles case where 'to' is a substring of 'from'
		}
		return str;
	}

}
namespace query {
	enum Type {
		SELECT,
		UPDATE,
		DELETE,
		INSERT
	};

	struct Target {
		std::vector<std::string> labels;
		Target(const std::vector<std::string>& labels = std::vector<std::string>()):labels(labels) {}
	};
	struct Predicate {
		std::function<bool(const Serialization::Serializable*)> predicate;
		Predicate(std::function<bool(const Serialization::Serializable*)> predicate = [](const Serialization::Serializable* obj)-> bool { return obj != nullptr; }):predicate(predicate){}
	};
	struct PayLoad {
		std::vector<Serialization::Serializable*> payLoad;
		PayLoad(const std::vector<Serialization::Serializable*>& objs = std::vector<Serialization::Serializable*>()):payLoad(objs){}
	};
	struct Query {
		Type type;
		Target target;
		Predicate predicate;
		PayLoad payLoad;
		Query():type(SELECT),target(),predicate(),payLoad(){}
		Query(Type type, Target&& target, Predicate&& predicate , PayLoad&& payLoad) :type(type), target(target), predicate(predicate) , payLoad(payLoad) {};
		void printQuery() {
			switch (type)
			{
			case query::SELECT: {
				std::cout << "SELECT ";
			}
				break;
			case query::UPDATE: {
				std::cout << "UPDATE ";
			}
				break;
			case query::DELETE: {
				std::cout << "DELETE ";
			}break;
			case query::INSERT: {
				std::cout << "INSERT ";
			}
				break;
			default: {
				std::cout << "UNDEFINED ";
			}
				break;
			}
			if (payLoad.payLoad.empty()) {
				std::cout << " * ";
			}
			else {
				for (const auto& label : target.labels) {
					std::cout << label << " ";
				}
			}
			std::cout << "WHERE predicate ";

			if (payLoad.payLoad.empty()) {
				std::cout << "WITH EMPTY PAYLOAD";
			}
			else {
				std::cout << "ON PAYLOAD";
			}
		}
	};
	class QueryBuilder {
	
	public:
		QueryBuilder(Type queryType) : query_(new Query()) {
			query_ = std::make_unique<Query>();
			query_->type = queryType;
		}

		QueryBuilder& setTarget(const std::vector<std::string>& labels) {
			query_->target.labels = labels;
			return *this;
		}

		QueryBuilder& setPredicate(const std::function<bool(const Serialization::Serializable*)>& predicate) {
			query_->predicate.predicate = predicate;
			return *this;
		}

		QueryBuilder& setPayLoad(std::vector<Serialization::Serializable*>&& payLoad) {
			query_->payLoad = PayLoad(payLoad);
			return *this;
		}

		Query build() {
			// You can perform additional validations before returning the built query
			query_->printQuery();
			return *query_;
		}

	private:
		std::unique_ptr<Query> query_;
	};
}
namespace table {

	class Cursor {
	private:
		fileIO::FileStream& m_fileStream;
		Serialization::Deserializer& m_deserializer;
		Serialization::Serializer& m_serializer;
		Serialization::FormatDescriptor& fd;
		std::map<std::string , int> m_mappedRows;
	public:
		Cursor(fileIO::FileStream& fileStream , Serialization::Deserializer& deserializer , Serialization::Serializer& serializer, Serialization::FormatDescriptor& fd);
		Cursor& operator=(const Cursor& other) {
			if (this != &other) {
				// ... implement the assignment logic ...
				m_fileStream = other.m_fileStream;
				m_deserializer = other.m_deserializer;
				m_serializer = other.m_serializer;
				fd = other.fd;
			}
			return *this;
		}
		std::vector<std::string> getPrimaryKeys();
		bool primaryKeyIsInside(const char* primaryKey)const noexcept;
		std::vector<Serialization::Serializable*> filterFields(const std::vector<size_t>& columnsIndexes, std::function<bool(const Serialization::Serializable*)>);
		void insertRows(std::vector<Serialization::Serializable*>content);
		void updateRow(Serialization::Serializable* newItem);
		void deleteRows(std::function<bool(const Serialization::Serializable*)>);
	};

	
	class Table {

	private: 
		Cursor m_cursor;
		std::vector<std::string> m_columnNames;
		std::string m_name;
	public:
		Table(Cursor& cursor, std::vector<std::string> columnNames, const char* tableName);
		std::optional<std::vector<Serialization::Serializable* >> executeQuery(query::Query& query);
		Table& operator=(const Table& other) {
			if (this != &other) {
				// ... implement the assignment logic ...
				m_cursor = other.m_cursor;
				m_columnNames = other.m_columnNames;
				m_name = other.m_name;
			}
			return *this;
		}
	};
}