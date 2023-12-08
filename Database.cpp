#include "Database.h"


fileIO::FileStream::FileStream(const char* path, const char* tag) : m_completePath(path), tag(tag) {
    fileStream.open(m_completePath, std::ios::in | std::ios::out | std::ios::app);
    if (!fileStream.is_open()) {
        std::cout << "Error opening file: " << m_completePath << std::endl;
    }
}

fileIO::FileStream::~FileStream() noexcept {
    if (fileStream.is_open()) {
        fileStream.close();
    }
}

fileIO::FileStream & fileIO::FileStream::operator=(const FileStream & other)
{
    if (this == &other) {
        return *this;
    }

    m_completePath = other.m_completePath;
    tag = other.tag;

    if (!fileStream.is_open() || fileStream.fail()) {
        fileStream.open(m_completePath, std::ios::in | std::ios::out | std::ios::app);
    }

    return *this;
}

fileIO::FileStream::FileStream(const FileStream& other) : m_completePath(other.m_completePath), tag(other.tag) {
    if (!fileStream.is_open() || fileStream.fail()) {
        fileStream.open(m_completePath, std::ios::in | std::ios::out | std::ios::app);
    }
}

#include <sstream>

bool fileIO::FileStream::deleteLine(size_t lineNumber, const char * delim)
{
    if (!fileStream.is_open() || fileStream.fail()) {
        std::cout << "Error opening file: " << m_completePath << std::endl;
        return false;
    }

    std::vector<std::string> lines = std::vector<std::string>();

    std::string line;
    int i = 0;
    
    while (getNextLine(line, delim)) {
       
     
        if (++i != lineNumber) lines.push_back(line);
        else {
            std::cout << "Line Skipped = " << line;
        }
    }

    for (const auto& l : lines) {
        putLine(l.c_str());
    }
}


bool fileIO::FileStream::overrideLine(size_t lineNumber, const char* newContent)
{ // Check if the line number is valid
    if (lineNumber == 0) {
        std::cout << "Invalid line number." << std::endl;
        return false;
    }

    // Store the current position in the file
    std::streampos originalPosition = fileStream.tellg();

    // Create a vector to store lines
    std::vector<std::string> lines;

    // Read lines from the file into the vector
    std::string line;
    while (std::getline(fileStream, line)) {
        lines.push_back(line);
    }

    // Check if the line number is within bounds
    if (lineNumber > lines.size()) {
        std::cout << "Line number out of bounds." << std::endl;
        // Restore the original position in the file
        fileStream.seekg(originalPosition);
        return false;
    }

    // Modify the specified line
    lines[lineNumber - 1] = newContent;

    // Clear the content of the file
    fileStream.clear();
    fileStream.seekp(0, std::ios::beg);

    // Write the modified lines back to the file
    for (const auto& modifiedLine : lines) {
        fileStream << modifiedLine << std::endl;
    }

    // Restore the original position in the file
    fileStream.seekg(originalPosition);

    return true;
}

void fileIO::FileStream::reopen()
{
    fileStream.close();
    fileStream.open(m_completePath);

    if (!fileStream.is_open()) {
        std::cerr << "Error opening file: " << m_completePath << std::endl;
        std::cerr << "Failbit: " << fileStream.fail() << " Badbit: " << fileStream.bad() << " Eofbit: " << fileStream.eof() << std::endl;
    }
   
}

void fileIO::FileStream::moveCarreteToEnd() noexcept {
    fileStream.seekg(0, std::ios::end);
}

   
void fileIO::FileStream::moveCarreteToLine(size_t where) {
    fileStream.seekg(std::streampos(where));
}

void fileIO::FileStream::moveCarreteToBegin() noexcept {
   
    fileStream.seekg(0, std::ios::beg);
    std::cout <<"Current Position = " << fileStream.tellg();
}

void fileIO::FileStream::putLine(const char* formattedLine) noexcept {
    fileStream << formattedLine;
}

const char* fileIO::FileStream::getTag() const noexcept
{
    return tag;
}

const char* fileIO::FileStream::getNextLine(std::string& dest, const char* delim) noexcept {
    if (std::getline(fileStream, dest)) {
        return dest.c_str();
    }
    else if (fileStream.eof() || fileStream.bad()) {
        return nullptr;
    }

   
}

std::string fileIO::FileStream::getFileContent()  noexcept {
    moveCarreteToBegin();
    std::stringstream buffer;
    buffer << fileStream.rdbuf();
    return buffer.str();
}





table::Cursor::Cursor(fileIO::FileStream& fileStream, Serialization::Deserializer& deserializer, Serialization::Serializer& serializer, Serialization::FormatDescriptor& formatDescriptor)
    : m_fileStream(fileStream), m_deserializer(deserializer), m_serializer(serializer), fd(formatDescriptor)
{
    int lineCount = 0;
    std::string line;
    while (m_fileStream.getNextLine(line, fd.getRowSeparator())) {
        auto firstDelim = std::find(line.begin(), line.end(), fd.getColumnSeparator());
        if (firstDelim != line.end()) {
            m_mappedRows[line.substr(0, firstDelim - line.begin())] = lineCount++;
        }
    }
}

std::vector<std::string> table::Cursor::getPrimaryKeys()
{
  //  auto kv = std::views::keys(m_mappedRows);
  //  return std::vector<std::string>{kv.begin(), kv.end()};
    return std::vector<std::string>();
}

bool table::Cursor::primaryKeyIsInside(const char* primaryKey) const noexcept
{
    return m_mappedRows.find(std::string(primaryKey)) != m_mappedRows.end();
}

std::vector<Serialization::Serializable*> table::Cursor::filterFields(const std::vector<size_t>& columnsIndexes, std::function<bool(const Serialization::Serializable*)> predicate) {
    // Vector to store the filtered Serializable objects
    std::vector<Serialization::Serializable*> result;

    // String to store each line read from the file
    std::string line;

    m_fileStream.moveCarreteToBegin();
    // Read lines from the file until the end
    while (m_fileStream.getNextLine(line, fd.getRowSeparator())) {
        // Deserialize the line into a vector of string fields
        auto parsedFields = this->m_deserializer.deserialize(line.c_str(), &fd);

        // Create a RowEntry object from the parsed fields
        auto entry = Serialization::RowEntry(parsedFields);

        // Check if the entry satisfies the predicate
        if (predicate(&entry)) {
            
            // Build a new Serializable with the fields that are inside the columnsIndexes
            std::vector<std::string> filteredFields;
            if (columnsIndexes.size() == 0) {
                filteredFields = parsedFields;
            } 
            else  for (auto index : columnsIndexes) {
                // Check if the index is within bounds
                if (index < parsedFields.size()) {
                    filteredFields.push_back(parsedFields[index]);
                   
                }
            }

            // Create a new RowEntry with the filtered fields
            Serialization::RowEntry* filteredEntry = new Serialization::RowEntry(filteredFields);

            // Add the filtered entry to the result vector
            result.push_back(filteredEntry);
        }
        line.clear();
    }

    // Return the vector of filtered Serializable objects
    return result;
}

void table::Cursor::insertRows(std::vector<Serialization::Serializable*> content)
{
    this->m_fileStream.moveCarreteToEnd();
    for (const auto& item : content) {
        
        bool keyCollision = this->primaryKeyIsInside(item->getPrimaryKey().c_str());
        
        if (keyCollision) {
            std::cout << "Key collision for primary key = " << item->getPrimaryKey();
            continue;
        }
        else {
                auto serialized = m_serializer.serialize(item, &fd);
                //mapping the key to it' position in table
                size_t index = m_mappedRows.size();
                this->m_mappedRows[item->getPrimaryKey()] =index;
                //writting the row into file stream
                m_fileStream.putLine(serialized.c_str());
        }
       
    }
    m_fileStream.moveCarreteToBegin();

}

void table::Cursor::updateRow(Serialization::Serializable* newItem)
{
    auto where = m_mappedRows.find(newItem->getPrimaryKey());
    if (where == m_mappedRows.end()) {
        std::cout << "Primary key not found" << newItem->getPrimaryKey() << "\n";
    }
    else {
        auto serialized = m_serializer.serialize(newItem, &fd);
        m_fileStream.overrideLine(m_mappedRows[newItem->getPrimaryKey()], serialized.c_str());
    }
}

void table::Cursor::deleteRows(std::function<bool(const Serialization::Serializable*)> predicate)
{
    // String to store each line read from the file
    std::string line;
    size_t lineIndex = 0;
   

   
    m_fileStream.reopen();  
    m_fileStream.moveCarreteToBegin();
    if (!m_fileStream.is_open() || m_fileStream.is_bad()) {
        std::cerr << "Error opening file for reading: " << m_fileStream.getPath() << std::endl;
        return;
    }

      

    while (m_fileStream.getNextLine(line, fd.getRowSeparator()) != nullptr) {
    
        // Deserialize the line into a vector of string fields
        auto parsedFields = this->m_deserializer.deserialize(line.c_str(), &fd);

        // Create a RowEntry object from the parsed fields
        auto entry = Serialization::RowEntry(parsedFields);

        // Check if the entry satisfies the predicate
        if (predicate(&entry)) {
            // Keep the line if it doesn't match the predicate
            m_fileStream.deleteLine(lineIndex , fd.getRowSeparator());
           
            std::cout << "\nRemoved row with pk = " << entry.getPrimaryKey();
        }
        
       
       
        lineIndex++;
    }
   
    

}

void Serialization::Serializer::sanitizeField( std::string& field , FormatDescriptor* fd)
{
    // Create a stringstream to build the sanitized field
    std::stringstream ss;

    // Start the result with a double quote
  

    // Replace field and row separators with substitutes
    field= util::ReplaceAll(field, std::string(fd->getColumnSeparator()),std::string( fd->getColumnSeparatorSubstitute()));
     field = util::ReplaceAll(field, std::string(fd->getRowSeparator()),std::string( fd->getRowSeparatorSubstitute()));

    // End the result with a double quote
   
}


std::string Serialization::Serializer::serialize(const Serializable* item, FormatDescriptor* fd)
{
    // Create a stringstream to build the serialized object
    std::stringstream ss;

    // Get the vector from the Persistable object
    auto vec = item->getContent();

    // Iterate over the vector and sanitize each field before appending to the stringstream
    for (auto iterator = vec.begin(); iterator != vec.end(); ++iterator) {
     sanitizeField(*iterator ,fd);
        if (*iterator != " ")
            ss << *iterator;

        // Check if it's not the last element before adding a comma
        if (std::next(iterator) != vec.cend()) {
            ss << ',';
        }
    }

    // Append the row separator and print the serialized object to the console
    ss << fd->getRowSeparator();
   

    // Return the serialized object as a string
    return ss.str();
}

void Serialization::Deserializer::removeSanitation(std::string& field , FormatDescriptor* fd)
{
   field = util::ReplaceAll(field, std::string(fd->getColumnSeparatorSubstitute()), std::string(fd->getColumnSeparator()));
     field = util::ReplaceAll(field, std::string(fd->getRowSeparatorSubstitute()), std::string(fd->getRowSeparator()));

    // Remove leading and trailing double quotes
   
}

std::vector<std::string> Serialization::Deserializer::deserialize(const char* line, FormatDescriptor* fd)
{
    // Initialize variables for tokenization
    size_t pos_start = 0, pos_end, delim_len = 1;
    std::string token;
    std::vector<std::string> res;
    auto st = std::string(line);
    // Tokenize the serialized object using field separator
    while ((pos_end = st.find(std::string(fd->getColumnSeparator()), pos_start)) != std::string::npos) {
        token = st.substr(pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        removeSanitation(token, fd);
        res.push_back(token);
    }

    // Add the last token after the last field separator
    auto last = st.substr(pos_start);
    removeSanitation(last, fd);
    res.push_back(last);

    // Create a new Persistable object from the parsed fields
    
    return res;
}

table::Table::Table(Cursor& cursor, std::vector<std::string> columnNames, const char* tableName):m_cursor(cursor) , m_columnNames(columnNames) , m_name(tableName)
{

}

std::optional<std::vector<Serialization::Serializable*>> table::Table::executeQuery(query::Query& query)
{
    switch (query.type)
    {
    case query::SELECT: {
        // Build index vector from labels
        std::vector<size_t> indexes;
        for (size_t i = 0; i < m_columnNames.size(); i++) {
            for (const auto& desiredField : query.target.labels) {
                if (desiredField == m_columnNames[i])
                    indexes.push_back(i);
            }
        }

        // Filter fields based on columns indexes and predicate
        auto result = m_cursor.filterFields(indexes, query.predicate.predicate);
        return result;
    }
                      break;
    case query::UPDATE: {
        std::cout << "UPDATE";
        if (query.payLoad.payLoad.empty()) {
            // Print a message if no payload is provided for UPDATE
            std::cout << "\nNo payload provided to UPDATE into TABLE " << this->m_name << "\n";
            break;
        }

        // Update rows in the cursor with the provided payload
        for (const auto& row : query.payLoad.payLoad)
            m_cursor.updateRow(row);
    }
                      break;
    case query::DELETE: {
        // Print a message indicating DELETE operation
        std::cout << "\nDELETE ";

        // Delete rows from the cursor based on the provided predicate
        m_cursor.deleteRows(query.predicate.predicate);
    }
                      break;
    case query::INSERT: {
        if (query.payLoad.payLoad.empty()) {
            // Print a message if no payload is provided for INSERT
            std::cout << "No payload provided to INSERT into TABLE " << this->m_name << "\n";
            break;
        }

        // Insert rows into the cursor with the provided payload
        m_cursor.insertRows(query.payLoad.payLoad);
    }
                      break;
    default: {
        // Print a message for an undefined query type
        std::cout << "UNDEFINED\n";
    }
           break;
    }

    // Return an empty optional by default
    return std::nullopt;
}

