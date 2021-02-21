#include "rvutil.hpp"

#include <iostream>
#include <iomanip>

using namespace std;
using namespace std::string_view_literals;
int list_pbo(filesystem::path file)
{
    rv::util::pbo::pbofile pbo(file);
    if (!pbo.good())
    {
        cout << "Reading in PBO '" << file << "' failed." << endl;
        return -1;
    }

    size_t max_meta_key_len = 0;
    for (auto it = pbo.metadatas_begin(); it != pbo.metadatas_end(); it++)
    {
        max_meta_key_len = max_meta_key_len > it->key.length()
            ? max_meta_key_len
            : it->key.length();
    }
    for (auto it = pbo.metadatas_begin(); it != pbo.metadatas_end(); it++)
    {
        cout << it->key << ": " << string(max_meta_key_len - it->key.length(), ' ') << it->value << '\n';
    }
    cout << endl;
    cout << "size actual" << " | " << "size original" << " | " << "file" << '\n';
    for (auto it = pbo.headers_begin(); it != pbo.headers_end(); it++)
    {
        cout << setw(11) << it->size_actual << " | " << setw(13) << it->size_original << " | " << it->name << '\n';
        if (it->is_empty_section()) { continue; }
        rv::util::pbo::pbofile::reader reader;
        if (pbo.read(*it, reader))
        {
            vector<char> data;
            data.resize(reader.size());
            reader.read(data.data(), data.size());
            cout << "<CONTENTS>\n" << string_view(data.data(), data.size()) << endl;
        }
        else
        {
            cout << "Failed to open reader of '" << it->name << "'" << endl;
            return -1;
        }
    }
    cout << endl;
    return 0;
}


int main(int argc, char **argv)
{
    filesystem::path file = "R:\\my.pbo";
    if (filesystem::exists(file))
    {
        list_pbo(file);
        filesystem::remove(file);
    }
    rv::util::pbo::pbofile pbo(file);
    if (!pbo.good())
    {
        cout << "Creating new PBO resulted in good flag not being set." << endl;
        return -1;
    }
    {
        rv::util::pbo::pbofile::writer writer;
        if (pbo.write("testfile1.txt", writer))
        {
            auto str = std::string(2000, '#');
            writer.write(str.data(), str.length());
        }
        else
        {
            cout << "Failed to open writer of 'testfile1.txt'" << endl;
            return -1;
        }
    }
    {
        rv::util::pbo::pbofile::writer writer;
        if (pbo.write("testfile2.txt", writer))
        {
            auto str = "this is another other test string"sv;
            writer.write(str.data(), str.length());
        }
        else
        {
            cout << "Failed to open writer of 'testfile2.txt'" << endl;
            return -1;
        }
    }
    {
        rv::util::pbo::pbofile::writer writer;
        if (pbo.write("testfile1.txt", writer))
        {
            auto str = "Ohhh data changed, SURPRISE BOOHOOOOO"sv;
            writer.write(str.data(), str.length());
            return 0;
        }
        else
        {
            cout << "Failed to open writer of 'testfile1.txt'" << endl;
            return -1;
        }
    }
}