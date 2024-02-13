# Usage: replace_all_in_zip.sh [search string] [replace string] [path to zip archive]

search_string=$1
replace_string=$2
archive=$3

set -x
set -o
# Create a new temporary directory.
tmp_dir=$( mktemp -d )
cp "$archive" "$tmp_dir"/archive.zip
(
cd $tmp_dir

# Use zipgrep to find all occurences of search_string.
for file in $(zipgrep -l "$search_string" archive.zip); do
    # Replace.
    unzip archive.zip "$file"
    sed -i "s/$search_string/$replace_string/g" "$file"
    zip -u archive.zip "$file"
done
)

# Copy the archive to the original directory, with a ".new.zip" suffix.
cp "$tmp_dir"/archive.zip "$archive".new.zip
