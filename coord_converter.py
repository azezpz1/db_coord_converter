import argparse, os, sqlite3, utm

class db_converter:
    def __init__(self, db_addr):
        db_dir = os.path.dirname(db_addr)
        conv_path = os.path.join(db_dir, 'coverted.db')
        conn = sqlite3.connect(db_addr)
        self.orig_c = conn.cursor()
        self.columns = [description[0] for description in self.orig_c.description]
        self.UTM_ZONE_ELEMENT = self.columns.index("utm_zone")
        self.UTM_EAST_ELEMENT = self.columns.index("utm_east")
        self.UTM_NORTH_ELEMENT = self.columns.index("utm_north")

        conn = sqlite3.connect(conv_path)
        self.conv_c = conn.cursor()

        # Create table
        self.conv_c.execute('''CREATE TABLE tx_markers (markernum TEXT, 
                            title TEXT, indexname TEXT, address TEXT, 
                            city TEXT, county TEXT, countyid TEXT, 
                            lat TEXT, long TEXT, code TEXT, year TEXT, 
                            rthl TEXT, loc_desc TEXT, size TEXT, 
                            marktertext TEXT, atlas_number TEXT)''')

    def next(self):
        row = self.orig_c.fetchone()
        zone = row[self.UTM_ZONE_ELEMENT]
        east = row[self.UTM_EAST_ELEMENT]
        north = row[self.UTM_NORTH_ELEMENT]
        row.pop(self.UTM_ZONE_ELEMENT) # removes unneeded element
        lat, lon = utm.to_latlon(east, north, zone, northern=True)
        row[self.UTM_EAST_ELEMENT] = lat
        row[self.UTM_NORTH_ELEMENT] = lon

        self.conv_c.execute('''INSERT INTO tx_markers VALUES (?,?,?,?,?,?,?,?,
                            ?,?,?,?,?,?,?,?)''', row)

    def db_length(self):
        # Returns the number of rows in the database
        rows = self.orig_c.execute('''SELECT * FROM tx_markers''')
        return len(rows)


def main():
    parser = argparse.ArgumentParser(description='Converts a database to lat/long')
    parser.add_argument("address", type=String)
    args = parser.parse_args()
    db_conv = db_converter(args.address)

if __name__ == '__main__':
    main()