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
    for (auto& [key, value] : pbo.attributes())
    {
        max_meta_key_len = max_meta_key_len > key.length()
            ? max_meta_key_len
            : key.length();
    }
    for (auto& [key, value] : pbo.attributes())
    {
        cout << key << ": " << string(max_meta_key_len - key.length(), ' ') << value << '\n';
    }
    cout << endl;
    cout << "size actual" << " | " << "size original" << " | " << "file" << '\n';
    for (auto& it : pbo.files())
    {
        cout << setw(11) << it.size << " | " << setw(13) << it.size << " | " << it.name << '\n';
        rv::util::pbo::pbofile::reader reader;
        if (pbo.read(it.name, reader))
        {
            vector<char> data;
            data.resize(it.size);
            reader.read(data.data(), data.size());
            cout << "<CONTENTS>\n" << string_view(data.data(), data.size()) << endl;
        }
        else
        {
            cout << "Failed to open reader of '" << it.name << "'" << endl;
            return -1;
        }
    }
    cout << endl;
    return 0;
}


int main(int argc, char **argv)
{
    filesystem::path original = "R:\\my.pbo";
    filesystem::path copy = "R:\\my.trunc.pbo";
    if (filesystem::exists(original))
    {
        list_pbo(original);
        filesystem::remove(original);
    }
    rv::util::pbo::pbofile pbo(original);
    if (!pbo.good())
    {
        cout << "Creating new PBO resulted in good flag not being set." << endl;
        return -1;
    }
    pbo.attribute("myAttributeRewrite", "attribute content");
    pbo.attribute("myAttribute1", "attribute content");
    pbo.attribute("myAttributeRewrite", "attribute content");
    pbo.attribute("myAttribute2", "attribute content");
    pbo.attribute("myAttribute3", "attribute content");
    pbo.attribute("myAttribute4", "attribute content");
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
            auto str = "-Ohhh data changed, SURPRISE BOOHOOOOO-"sv;
            writer.write(str.data(), str.length());
            writer.truncate();
        }
        else
        {
            cout << "Failed to open writer of 'testfile1.txt'" << endl;
            return -1;
        }
    }
    {
        rv::util::pbo::pbofile::writer writer;
        if (pbo.write("testfile3.txt", writer))
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
    pbo.copy_truncated(copy);
}