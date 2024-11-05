#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <opencv2/opencv.hpp>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stack>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#define UNUSED(p) ((void)(p))

#define ASSERT_WITH_MESSAGE(condition, message)                                \
  do {                                                                         \
    if (!(condition)) {                                                        \
      std::cerr << "Assertion \033[1;31mFAILED\033[0m: " << message << " at "  \
                << __FILE__ << ":" << __LINE__ << std::endl;                   \
      std::abort();                                                            \
    }                                                                          \
  } while (0)

enum FieldType { INT, FLOAT, STRING };

// Define a basic Field variant class that can hold different types
class Field {
public:
  FieldType type;
  std::unique_ptr<char[]> data;
  size_t data_length;

public:
  Field(int i) : type(INT) {
    data_length = sizeof(int);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &i, data_length);
  }

  Field(float f) : type(FLOAT) {
    data_length = sizeof(float);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &f, data_length);
  }

  Field(const std::string &s) : type(STRING) {
    data_length = s.size() + 1; // include null-terminator
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), s.c_str(), data_length);
  }

  Field &operator=(const Field &other) {
    if (&other == this) {
      return *this;
    }
    type = other.type;
    data_length = other.data_length;
    std::memcpy(data.get(), other.data.get(), data_length);
    return *this;
  }

  Field(Field &&other) {
    type = other.type;
    data_length = other.data_length;
    std::memcpy(data.get(), other.data.get(), data_length);
  }

  FieldType getType() const { return type; }
  int asInt() const { return *reinterpret_cast<int *>(data.get()); }
  float asFloat() const { return *reinterpret_cast<float *>(data.get()); }
  std::string asString() const { return std::string(data.get()); }

  std::string serialize() {
    std::stringstream buffer;
    buffer << type << ' ' << data_length << ' ';
    if (type == STRING) {
      buffer << data.get() << ' ';
    } else if (type == INT) {
      buffer << *reinterpret_cast<int *>(data.get()) << ' ';
    } else if (type == FLOAT) {
      buffer << *reinterpret_cast<float *>(data.get()) << ' ';
    }
    return buffer.str();
  }

  void serialize(std::ofstream &out) {
    std::string serializedData = this->serialize();
    out << serializedData;
  }

  static std::unique_ptr<Field> deserialize(std::istream &in) {
    int type;
    in >> type;
    size_t length;
    in >> length;
    if (type == STRING) {
      std::string val;
      in >> val;
      return std::make_unique<Field>(val);
    } else if (type == INT) {
      int val;
      in >> val;
      return std::make_unique<Field>(val);
    } else if (type == FLOAT) {
      float val;
      in >> val;
      return std::make_unique<Field>(val);
    }
    return nullptr;
  }

  void print() const {
    switch (getType()) {
    case INT:
      std::cout << asInt();
      break;
    case FLOAT:
      std::cout << asFloat();
      break;
    case STRING:
      std::cout << asString();
      break;
    }
  }
};

class Tuple {
public:
  std::vector<std::unique_ptr<Field>> fields;

  void addField(std::unique_ptr<Field> field) {
    fields.push_back(std::move(field));
  }

  size_t getSize() const {
    size_t size = 0;
    for (const auto &field : fields) {
      size += field->data_length;
    }
    return size;
  }

  std::string serialize() {
    std::stringstream buffer;
    buffer << fields.size() << ' ';
    for (const auto &field : fields) {
      buffer << field->serialize();
    }
    return buffer.str();
  }

  void serialize(std::ofstream &out) {
    std::string serializedData = this->serialize();
    out << serializedData;
  }

  static std::unique_ptr<Tuple> deserialize(std::istream &in) {
    auto tuple = std::make_unique<Tuple>();
    size_t fieldCount;
    in >> fieldCount;
    for (size_t i = 0; i < fieldCount; ++i) {
      tuple->addField(Field::deserialize(in));
    }
    return tuple;
  }

  void print() const {
    for (const auto &field : fields) {
      field->print();
      std::cout << " ";
    }
    std::cout << "\n";
  }
};

static constexpr size_t PAGE_SIZE = 4096; // Fixed page size
static constexpr size_t MAX_SLOTS = 512;  // Fixed number of slots
static constexpr size_t MAX_PAGES =
    1000; // Total Number of pages that can be stored
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max(); // Sentinel value

struct Slot {
  bool empty = true;               // Is the slot empty?
  uint16_t offset = INVALID_VALUE; // Offset of the slot within the page
  uint16_t length = INVALID_VALUE; // Length of the slot
};

// Slotted Page class
class SlottedPage {
public:
  std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
  size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

  SlottedPage() {
    // Empty page -> initialize slot array inside page
    Slot *slot_array = reinterpret_cast<Slot *>(page_data.get());
    for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
      slot_array[slot_itr].empty = true;
      slot_array[slot_itr].offset = INVALID_VALUE;
      slot_array[slot_itr].length = INVALID_VALUE;
    }
  }

  // Add a tuple, returns true if it fits, false otherwise.
  bool addTuple(std::unique_ptr<Tuple> tuple) {

    // Serialize the tuple into a char array
    auto serializedTuple = tuple->serialize();
    size_t tuple_size = serializedTuple.size();

    // std::cout << "Tuple size: " << tuple_size << " bytes\n";
    assert(tuple_size == 38);

    // Check for first slot with enough space
    size_t slot_itr = 0;
    Slot *slot_array = reinterpret_cast<Slot *>(page_data.get());
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_array[slot_itr].empty == true and
          slot_array[slot_itr].length >= tuple_size) {
        break;
      }
    }
    if (slot_itr == MAX_SLOTS) {
      // std::cout << "Page does not contain an empty slot with sufficient space
      // to store the tuple.";
      return false;
    }

    // Identify the offset where the tuple will be placed in the page
    // Update slot meta-data if needed
    slot_array[slot_itr].empty = false;
    size_t offset = INVALID_VALUE;
    if (slot_array[slot_itr].offset == INVALID_VALUE) {
      if (slot_itr != 0) {
        auto prev_slot_offset = slot_array[slot_itr - 1].offset;
        auto prev_slot_length = slot_array[slot_itr - 1].length;
        offset = prev_slot_offset + prev_slot_length;
      } else {
        offset = metadata_size;
      }

      slot_array[slot_itr].offset = offset;
    } else {
      offset = slot_array[slot_itr].offset;
    }

    if (offset + tuple_size >= PAGE_SIZE) {
      slot_array[slot_itr].empty = true;
      slot_array[slot_itr].offset = INVALID_VALUE;
      return false;
    }

    assert(offset != INVALID_VALUE);
    assert(offset >= metadata_size);
    assert(offset + tuple_size < PAGE_SIZE);

    if (slot_array[slot_itr].length == INVALID_VALUE) {
      slot_array[slot_itr].length = tuple_size;
    }

    // Copy serialized data into the page
    std::memcpy(page_data.get() + offset, serializedTuple.c_str(), tuple_size);

    return true;
  }

  void deleteTuple(size_t index) {
    Slot *slot_array = reinterpret_cast<Slot *>(page_data.get());
    size_t slot_itr = 0;
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_itr == index and slot_array[slot_itr].empty == false) {
        slot_array[slot_itr].empty = true;
        break;
      }
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void print() const {
    Slot *slot_array = reinterpret_cast<Slot *>(page_data.get());
    for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_array[slot_itr].empty == false) {
        assert(slot_array[slot_itr].offset != INVALID_VALUE);
        const char *tuple_data = page_data.get() + slot_array[slot_itr].offset;
        std::istringstream iss(tuple_data);
        auto loadedTuple = Tuple::deserialize(iss);
        std::cout << "Slot " << slot_itr << " : [";
        std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
        loadedTuple->print();
      }
    }
    std::cout << "\n";
  }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
public:
  std::fstream fileStream;
  size_t num_pages = 0;
  std::mutex io_mutex;

public:
  StorageManager(bool truncate_mode = true) {
    auto flags = truncate_mode ? std::ios::in | std::ios::out | std::ios::trunc
                               : std::ios::in | std::ios::out;
    fileStream.open(database_filename, flags);
    if (!fileStream) {
      // If file does not exist, create it
      fileStream.clear(); // Reset the state
      fileStream.open(database_filename, truncate_mode
                                             ? (std::ios::out | std::ios::trunc)
                                             : std::ios::out);
    }
    fileStream.close();
    fileStream.open(database_filename, std::ios::in | std::ios::out);

    fileStream.seekg(0, std::ios::end);
    num_pages = fileStream.tellg() / PAGE_SIZE;

    if (num_pages == 0) {
      extend();
    }
  }

  ~StorageManager() {
    if (fileStream.is_open()) {
      fileStream.close();
    }
  }

  // Read a page from disk
  std::unique_ptr<SlottedPage> load(uint16_t page_id) {
    fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
    auto page = std::make_unique<SlottedPage>();
    // Read the content of the file into the page
    if (fileStream.read(page->page_data.get(), PAGE_SIZE)) {
      // std::cout << "Page read successfully from file." << std::endl;
    } else {
      std::cerr << "Error: Unable to read data from the file. \n";
      exit(-1);
    }
    return page;
  }

  // Write a page to disk
  void flush(uint16_t page_id, const SlottedPage &page) {
    size_t page_offset = page_id * PAGE_SIZE;

    // Move the write pointer
    fileStream.seekp(page_offset, std::ios::beg);
    fileStream.write(page.page_data.get(), PAGE_SIZE);
    fileStream.flush();
  }

  // Extend database file by one page
  void extend() {
    // Create a slotted page
    auto empty_slotted_page = std::make_unique<SlottedPage>();

    // Move the write pointer
    fileStream.seekp(0, std::ios::end);

    // Write the page to the file, extending it
    fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
    fileStream.flush();

    // Update number of pages
    num_pages += 1;
  }

  void extend(uint64_t till_page_id) {
    std::lock_guard<std::mutex> io_guard(io_mutex);
    uint64_t write_size =
        std::max(static_cast<uint64_t>(0), till_page_id + 1 - num_pages) *
        PAGE_SIZE;
    if (write_size > 0) {
      // std::cout << "Extending database file till page id : "<<till_page_id<<"
      // \n";
      char *buffer = new char[write_size];
      std::memset(buffer, 0, write_size);

      fileStream.seekp(0, std::ios::end);
      fileStream.write(buffer, write_size);
      fileStream.flush();

      num_pages = till_page_id + 1;
    }
  }
};

using PageID = uint16_t;

class Policy {
public:
  virtual bool touch(PageID page_id) = 0;
  virtual PageID evict() = 0;
  virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID> &myList) {
  std::cout << list_name << " :: ";
  for (const PageID &value : myList) {
    std::cout << value << ' ';
  }
  std::cout << '\n';
}

class LruPolicy : public Policy {
private:
  // List to keep track of the order of use
  std::list<PageID> lruList;

  // Map to find a page's iterator in the list efficiently
  std::unordered_map<PageID, std::list<PageID>::iterator> map;

  size_t cacheSize;

public:
  LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

  bool touch(PageID page_id) override {
    // printList("LRU", lruList);

    bool found = false;
    // If page already in the list, remove it
    if (map.find(page_id) != map.end()) {
      found = true;
      lruList.erase(map[page_id]);
      map.erase(page_id);
    }

    // If cache is full, evict
    if (lruList.size() == cacheSize) {
      evict();
    }

    if (lruList.size() < cacheSize) {
      // Add the page to the front of the list
      lruList.emplace_front(page_id);
      map[page_id] = lruList.begin();
    }

    return found;
  }

  PageID evict() override {
    // Evict the least recently used page
    PageID evictedPageId = INVALID_VALUE;
    if (lruList.size() != 0) {
      evictedPageId = lruList.back();
      map.erase(evictedPageId);
      lruList.pop_back();
    }
    return evictedPageId;
  }
};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
private:
  using PageMap = std::unordered_map<PageID, SlottedPage>;

  StorageManager storage_manager;
  PageMap pageMap;
  std::unique_ptr<Policy> policy;

public:
  BufferManager(bool storage_manager_truncate_mode = true)
      : storage_manager(storage_manager_truncate_mode),
        policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {
    storage_manager.extend(MAX_PAGES);
  }

  ~BufferManager() {
    for (auto &pair : pageMap) {
      flushPage(pair.first);
    }
  }

  SlottedPage &fix_page(int page_id) {
    auto it = pageMap.find(page_id);
    if (it != pageMap.end()) {
      policy->touch(page_id);
      return pageMap.find(page_id)->second;
    }

    if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
      auto evictedPageId = policy->evict();
      if (evictedPageId != INVALID_VALUE) {
        // std::cout << "Evicting page " << evictedPageId << "\n";
        storage_manager.flush(evictedPageId, pageMap[evictedPageId]);
      }
    }

    auto page = storage_manager.load(page_id);
    policy->touch(page_id);
    // std::cout << "Loading page: " << page_id << "\n";
    pageMap[page_id] = std::move(*page);
    return pageMap[page_id];
  }

  void flushPage(int page_id) {
    storage_manager.flush(page_id, pageMap[page_id]);
  }

  void extend() { storage_manager.extend(); }

  size_t getNumPages() { return storage_manager.num_pages; }
};

struct Data {
  int size = 1024 - 2;
  int page_id;
  int data[1024 - 2];
};

class Compressor {
public:
  int next_page_id;
  BufferManager &buffer_manager;

  Compressor(BufferManager &buffer_manager) : buffer_manager(buffer_manager) {
    next_page_id = 1;
  }

  Data *createData() {
    SlottedPage *page = &buffer_manager.fix_page(next_page_id++);
    Data *data = reinterpret_cast<Data *>(page->page_data.get());
    data->size = (PAGE_SIZE - 2 * sizeof(int)) / 4;
    data->page_id = next_page_id - 1;
    return data;
  }

  Data *getData(int id) {
    SlottedPage *page = &buffer_manager.fix_page(id);
    return reinterpret_cast<Data *>(page->page_data.get());
  }

  cv::Mat *createMat() {
    SlottedPage *page = &buffer_manager.fix_page(next_page_id++);
    return reinterpret_cast<cv::Mat *>(page->page_data.get());
  }

  cv::Mat *getMat(int id) {
    SlottedPage *page = &buffer_manager.fix_page(id);
    return reinterpret_cast<cv::Mat *>(page->page_data.get());
  }

  int createIntRLE(int *src, int *dst, int numInts) {
    int i = 0;
    int newSize = 0;
    int current, currentCount;
    while (i < numInts) {
      current = *src;
      currentCount = 0;
      while (*src == current && i < numInts) {
        ++src;
        ++currentCount;
        ++i;
      }
      *dst = current;
      *(dst + 1) = currentCount;
      newSize += 2;
      dst += 2;
    }
    return newSize;
  }

  int decodeRLE(int *src, int *dst, int numInts) {
    int i = 0;
    int newSize = 0;
    int current, currentCount;
    while (i < numInts) {
      current = *src;
      currentCount = *(src + 1);
      while (currentCount > 0) {
        *dst = current;
        dst++;
        --currentCount;
        ++newSize;
      }
      src += 2;
      i += 2;
    }
    return newSize;
  }

  std::unordered_map<std::string, int> encodeLZW(char *src, char *dst, int size,
                                                 int *outputSize) {
    int count = 0;
    std::string current = "";
    std::unordered_map<std::string, int> result;
    while (count < size) {
      if (result.find(current + *(dst + count)) != result.end()) {
        current += *(dst + count);
      } else {
        result[current + *(dst + count)] = 1;
        current = *(dst + count);
      }
      count++;
    }
    return result;
  }

  struct Node {
    Node *left;
    Node *right;
    char value;
    int frequency;
    bool isLeaf;
    Node(Node *left, Node *right, char value, int frequency, bool isLeaf) {
      this->left = left;
      this->right = right;
      this->value = value;
      this->frequency = frequency;
      this->isLeaf = isLeaf;
    }
  };

  void TreeDestructor(Node *node) {
    if (!node->isLeaf) {
      TreeDestructor(node->left);
      TreeDestructor(node->right);
    }
    delete node;
  }

  std::unordered_map<char, std::string>
  encodeHuffman(char *src, char *dst, int numChars, int *numBits) {
    int size = 0;

    // Construct a frequency mapping.
    std::unordered_map<char, int> mapping;
    for (int i = 0; i < numChars; ++i) {
      mapping[*(src + i)]++;
    }

    // Create priority queue to sort the elements
    typedef std::pair<char, int> value;

    auto comp = [](value a, value b) { return a.second > b.second; };
    std::priority_queue<value, std::vector<value>, decltype(comp)> pq(comp);
    for (const auto &[k, v] : mapping) {
      pq.push(std::make_pair(k, v));
      std::cout << k << " " << v << std::endl;
    }
    std::vector<Node> nodes;
    Node *firstLeft =
        new Node(nullptr, nullptr, pq.top().first, pq.top().second, true);
    pq.pop();
    Node *firstRight =
        new Node(nullptr, nullptr, pq.top().first, pq.top().second, true);
    pq.pop();
    Node *currentFakeNode =
        new Node(firstLeft, firstRight, 'z',
                 firstLeft->frequency + firstRight->frequency, false);
    Node *root = currentFakeNode;
    int height = 1;
    while (pq.size()) {
      Node *newLeaf =
          new Node(nullptr, nullptr, pq.top().first, pq.top().second, true);
      pq.pop();
      Node *newFakeNode = new Node(
          currentFakeNode->frequency < newLeaf->frequency ? currentFakeNode
                                                          : newLeaf,
          currentFakeNode->frequency < newLeaf->frequency ? newLeaf
                                                          : currentFakeNode,
          'z', currentFakeNode->frequency + newLeaf->frequency, false);
      root = newFakeNode;
      currentFakeNode = newFakeNode;
      height++;
    }

    std::unordered_map<char, std::string> encodings;
    Node *current = root;
    std::string currentEncoding = "";
    while (true) {
      if (current->left->isLeaf && current->right->isLeaf) {
        encodings[current->left->value] = currentEncoding + "0";
        encodings[current->right->value] = currentEncoding + "1";
        std::cout << "Bottom node with value " << current->left->value
                  << " and frequency: " << current->left->frequency
                  << std::endl;
        std::cout << "Bottom node with value " << current->right->value
                  << " and frequency: " << current->right->frequency
                  << std::endl;
        break;
      }
      if (current->left->isLeaf) {
        encodings[current->left->value] = currentEncoding + "0";
        current = current->right;
        currentEncoding += "1";
        std::cout << "Leaf node with value " << current->left->value
                  << " and frequency: " << current->left->frequency
                  << std::endl;
      } else if (current->right->isLeaf) {
        encodings[current->right->value] = currentEncoding + "1";
        current = current->left;
        currentEncoding += "0";
        std::cout << "Leaf node with value " << current->right->value
                  << " and frequency: " << current->right->frequency
                  << std::endl;
      } else {
        ASSERT_WITH_MESSAGE(false,
                            "Both children of a node are not leaves. Try "
                            "checking if the tree was generated correctly");
      }
    }
    ASSERT_WITH_MESSAGE(mapping.size() == encodings.size(),
                        "Mismatched encoding mapping size. Try checking if the "
                        "tree was generated correctly");

    int currentByte = 0;
    int currentBit = 0;
    for (int i = 0; i < numChars; ++i) {
      std::string currentEncoding = encodings[*(src + i)];
      for (int j = 0; j < currentEncoding.size(); ++j) {
        if (currentBit == 0) {
          *(dst + currentByte) = 0;
        }
        *(dst + currentByte) |= (1 << currentBit);
        currentBit++;
        *numBits = *numBits + 1;
        if (currentBit == 8) {
          currentBit = 0;
          currentByte++;
        }
      }
    }
    TreeDestructor(root);
    return encodings;
  }
};

int main(int argc, char *argv[]) {
  UNUSED(argc);
  UNUSED(argv);
  BufferManager buffer_manager;
  Compressor compressor(buffer_manager);

  // Test the buffer manager
  Data *data = compressor.createData();
  // for (int i = 0; i < data->size; ++i) {
  //     data->data[i] = i;
  // }
  // for (int i = 0; i < data->size; ++i) {
  //     std::cout << data->data[i] << std::endl;
  // }

  cv::Mat *image = compressor.createMat();
  *image = cv::imread("./../images.jpeg");
  // for (int i = 0; i < sizeof(*image / 4); ++i) {
  //     std::cout << *((int*)(image) + i) << std::endl;
  // }
  cv::Mat *image2 = compressor.getMat(compressor.next_page_id - 1);
  int buffer[sizeof(*image2 + 1)];

  char *buffer2 = "Helllllo world!";
  std::cout << "the letter is: " << *buffer2 << std::endl;
  int size3 = 0;
  std::unordered_map<char, std::string> encodings =
      compressor.encodeHuffman(buffer2, (char *)buffer, 15, &size3);

  int size =
      compressor.createIntRLE((int *)image2, buffer, sizeof(*image2) / 4);
  // for (int *i = (int *)image2; i < ((int *)image2) + (sizeof(*image2) / 4);
  //      ++i) {
  //   std::cout << *i << std::endl;
  // }
  // std::cout << "-------------------------------" << std::endl;
  // for (int i = 0; i < size; ++i) {
  //   std::cout << buffer[i] << std::endl;
  // }
  //
  int size2 = compressor.decodeRLE(buffer, (int *)image2, size);
  // std::cout << "-------------------------------" << std::endl;
  // for (int *i = (int *)image2; i < ((int *)image2) + (sizeof(*image2) / 4);
  //      ++i) {
  //   std::cout << *i << std::endl;
  // }

  // std::cout << "Size Initial: " << sizeof(*image2) << std::endl;
  // std::cout << "Size Compressed: " << size * 4 << std::endl;
  // std::cout << "Size Not Compressed: " << size2 * 4 << std::endl;
  //
  // std::cout << "Rows: " << image2->rows << std::endl;
  // std::cout << "Cols: " << image2->cols << std::endl;
  // std::cout << "Size: " << sizeof(*image2) << std::endl;
  cv::namedWindow("Display Image", cv::WINDOW_AUTOSIZE);
  cv::imshow("Display Image", *image2);
  cv::waitKey(0);

  return 0;
}
