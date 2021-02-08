#!bin/bash

# 08.02.2021
# The script is used to download a zip file from an indicated location. After unzipping the   	file, the script cleans the data from empty and redundant.
# After cleaning the data the script uploads it to mysql database and sends an e-mail with $
# the report.

# Parametry:
# parametr 1 - index number
# parametr 2 - date
# parametr 3 - download file url
# parametr 4 - archive password
# parametr 5 - path to InternetSales_old.txt
# parametr 6 - mysql hostname
# parametr 7 - mysql database name
# parametr 8 - mysql password

# Usage:
# bash skrypt.sh <index number> <data> <log name> <downloaded file> <archive password> <mysql hostname> <mysql database name> <mysql password>


# Create PROCESSED directory
echo "1. Create PROCESSED directory"
mkdir PROCESSED
logfile_name="$0_$2_logfile"
touch PROCESSED/$logfile_name

# Download file
echo "2. Download file"
wget -cq "$3"  
curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - File downloaded - Successful" >> PROCESSED/$logfile_name

file_name=$(basename "$3")
unzip -P "$4" $file_name -d unziped_file 
curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - File extracted - Successful" >> PROCESSED/$logfile_name

files=(unziped_file/*)
file_name_txt=$(basename "${files[0]}" )
file_path_txt=unziped_file/${file_name_txt}
row_count=$(wc -l < "$file_path_txt")
echo -e "File has: $row_count rows" >> PROCESSED/$logfile_name

# Remove empty lines
echo "3. Remove empty lines"
sed -i '/^$/d' $file_path_txt
curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed empty lines - Successful" >> PROCESSED/$logfile_name

rows_not_empty_count=$(wc -l < "$file_path_txt")
rows_not_empty_count=$(($rows_not_empty_count-1))
empty_rows="$(($row_count-$rows_not_empty_count))"
echo -e "File has: $empty_rows empty rows" >> PROCESSED/$logfile_name

# Remove duplicates
echo "4. Remove duplicates"
filename_no_extension=$(basename $file_name_txt .txt)
badfile_name=${filename_no_extension}".bad_$2"
awk 'NR == 1; NR>1 {print $0 |"sort -n"}' "$file_path_txt" > sorted_temp  
uniq -d sorted_temp  >> "$badfile_name"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed duplicates - Successful" >> PROCESSED/$logfile_name

uniq -u sorted_temp > "$file_path_txt"
rm sorted_temp

duplicated_rows=$(wc -l < "$badfile_name")
mail_duplicated="File has: $duplicated_rows duplicated rows."
echo -e "$mail_duplicated" >> PROCESSED/$logfile_name

# Remove invalid column numbers rows
echo "5. Remove invalid column numbers rows"
awk -v n="7" -F'|' 'NF!=n ' "$file_path_txt" > "$badfile_name"     
awk -v n="7" -F'|' 'NF==n ' "$file_path_txt" > temp_awk
mv temp_awk "$file_path_txt" 

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed invalid rows - Successful" >> PROCESSED/$logfile_name


bad_rows=$(wc -l < "$badfile_name")
invalid_ncol="$(($bad_rows-$duplicated_rows))" 				
echo -e "File has: $invalid_ncol invalid rows" >> PROCESSED/$logfile_name

# Remove records with invalid OrderQuantity
echo "6. Remove records with invalid OrderQuantity"
awk -v val=100 -F '|' '$3 > val || $3 == ""' "$file_path_txt" |tail -n +2 >> $badfile_name
head -n 1 "$file_path_txt" > header_line
cat header_line > tmp_quantity
awk -v val=100 -F '|' '$3 <= val && $3 != "" ' "$file_path_txt"  >> tmp_quantity

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed records with wrong OrderQuantity rows - Successful" >> PROCESSED/$logfile_name

mv tmp_quantity "$file_path_txt"

bad_rows_quantity=$(wc -l < "$badfile_name")
bad_quantity_rows="$(($bad_rows_quantity-$bad_rows))"
echo -e "File has: $bad_quantity_rows bad OrderQuantity rows" >> PROCESSED/$logfile_name

# Compare data with InternetSales_old.txt
echo "7. Compare data with InternetSales_old.txt"
tail -n +2 "$file_path_txt" | sort > sorted_new_temp	
tail -n +2 "$5" | sort > sorted_old_temp

diff  sorted_old_temp sorted_new_temp  --changed-group-format=""  >> $badfile_name

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removing existing rows - Successful" >> PROCESSED/$logfile_name

bad_rows_existing=$(wc -l < "$badfile_name")
existing_rows="$(($bad_rows_existing-$bad_rows_quantity))"
echo -e "Already existing rows: $existing_rows" >> PROCESSED/$logfile_name

cat header_line > tmp_after_check 
diff sorted_old_temp sorted_new_temp --old-group-format=""  --unchanged-group-format=""  >> tmp_after_check
mv tmp_after_check "$file_path_txt"
rm sorted_old_temp 
rm sorted_new_temp

# Remove rows with SecretCode value
echo "8. Remove rows with invalid SecretCode values"
awk  -F '|' '$7 !=""' "$file_path_txt" | tail -n +2 | cut -d '|' -f -6 | awk '{print $0"|"}' >> "$badfile_name"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed lines with secret code present - Successful" >> PROCESSED/$logfile_name

bad_rows_sc=$(wc -l < "$badfile_name")
rows_sc="$(($bad_rows_sc-$bad_rows_existing))"
echo "File has $rows_sc rows with secret_code column not empty" >> PROCESSED/$logfile_name

cat header_line > tmp_non_secret_code
tail -n +2 $file_path_txt | awk  -F '|' '$7 ==""' >> tmp_non_secret_code 
mv tmp_non_secret_code "$file_path_txt"

# Remove lines with invalid name and surname
echo "9. Remove lines with invalid name and surname"
awk -F"|" '!match($3,",") ' "$file_path_txt" | tail -n +2  >> "$badfile_name"

cat header_line > tmp_invalid_name
awk -F"|" 'match($3,",") ' "$file_path_txt"  >> tmp_invalid_name

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Removed lines with missing comma - Successful" >> PROCESSED/$logfile_name

mv tmp_invalid_name "$file_path_txt"
bad_rows_invalid_name=$(wc -l < "$badfile_name")
rows_invalid_name="$(($bad_rows_invalid_name-$bad_rows_sc))"
mail_all_bad="File has $bad_rows_invalid_name invalid rows"
echo -e "File has $rows_invalid_name rows with invalid name and surname" >> PROCESSED/$logfile_name
rm header_line

# Split first name and last name into different columns
echo "10. Split first name and last name into different columns"
echo "FIRST_NAME" > first_name
echo "LAST_NAME" > last_name
cut -d'|' -f-2 "$file_path_txt" > first_col
cut -d'|' -f4- "$file_path_txt"  > last_col
cut -d'|' -f3 "$file_path_txt" | tr -d "\""| cut -d','  -f2 |tail -n +2 >> first_name
cut -d'|' -f3 "$file_path_txt" | tr -d "\""| cut -d','  -f1 |tail -n +2 >> last_name 
paste -d'|' first_col first_name last_name last_col > "$file_path_txt"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Splitting column Customer_name to first and last name  - Successful" >> PROCESSED/$logfile_name

rm first_name 
rm last_name 
rm first_col 
rm last_col

# Create table in MySql database
echo "11. Create table in MySql database"
column1=$(head -n1 "$file_path_txt" | cut -d '|' -f1)
column2=$(head -n1 "$file_path_txt" | cut -d '|' -f2)
column3=$(head -n1 "$file_path_txt" | cut -d '|' -f3)
column4=$(head -n1 "$file_path_txt" | cut -d '|' -f4)
column5=$(head -n1 "$file_path_txt" | cut -d '|' -f5)
column6=$(head -n1 "$file_path_txt" | cut -d '|' -f6)
column7=$(head -n1 "$file_path_txt" | cut -d '|' -f7)
column8=$(head -n1 "$file_path_txt" | cut -d '|' -f8)

table_name="CUSTOMERS_$1"
mysql -u "$7" -h  "$6" -P 3306 -D "$7" --silent -e "CREATE TABLE $table_name($column1 INTEGER, $column2 VARCHAR(50), $column3 VARCHAR(50), $column4 VARCHAR(50), $column5 VARCHAR(50), $column6 VARCHAR(50), $column7 FLOAT, $column8 VARCHAR(50) );"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Creating table in db - Successful" >> PROCESSED/$logfile_name

tail +2 "$file_path_txt" | tr ',' '.' > file_no_header
mv "$file_path_txt" PROCESSED/
mysql --local-infile=1 -u "$7" -h  "$6" -P 3306 -D "$7" --silent -e "LOAD DATA LOCAL INFILE 'file_no_header' INTO TABLE $table_name FIELDS TERMINATED BY '|';"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Inserting data to database table - Successful" >> PROCESSED/$logfile_name

rm file_no_header

# Change SecretCode values
echo "12. Set SecretCode values"
random="$(openssl rand -hex 5)"
mysql -u "$7" -h  "$6" -P 3306 -D "$7" --silent -e "UPDATE $table_name SET $column8='$random';"

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Updatind table  - Successful" >> PROCESSED/$logfile_name

# Export data to .csv file
echo "13. Export data to CSV file"
mysql -u "$7" -h  "$6" -P 3306 -D "$7" --silent -e "SELECT * FROM $table_name;" | sed 's/\t/,/g' > $table_name.csv

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Exporting table to .csv file  - Successful" >> PROCESSED/$logfile_name

# Compress .csv file
zip -q $table_name $table_name.csv  

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Zipping .csv file - Successful" >> PROCESSED/$logfile_name

# Send emails with attachment
echo "14. Send e-mails"
mail_user="kmarcjan@student.agh.edu.pl"
mail_all_bad="File has got $all_bad_lines_name_surname invalid rows"
good_rows="$(($row_count-$bad_rows_invalid_name))"
mail_all_good="File has $good_rows valid rows"
table_count=$(mysql -u "$7" -h  "$6" -P 3306 -D "$7" --silent -e "SELECT COUNT(*) FROM $table_name;")
mail_insert="Inserted $table_count rows to database"

echo -e "RAPORT: \n$mail_downloaded\n$mail_all_good \n$mail_duplicated \n$mail_all_bad \n$mail_insert" | mailx -s "CUSTOMERS LOAD - $2, " -A PROCESSED/$logfile_name "$mail_user"
mailx -s "Date:$2, $good_rows" -A $table_name.zip "$mail_user" 

curr_time=$(date '+%Y%m%d%H%M%S')
echo "$curr_time - Email sent - Successful" >> PROCESSED/$logfile_name
