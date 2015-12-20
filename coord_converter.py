import argparse, os, sqlite3, utm

class db_converter:
    ROW_NUM = 16090

    def __init__(self, db_addr):
        db_dir = os.path.dirname(db_addr)
        conv_path = os.path.join(db_dir, 'converted.db')
        if os.path.isfile(conv_path):
            os.remove(conv_path)
        conn = sqlite3.connect(db_addr)
        self.orig_c = conn.cursor()
        self.columns = ["markernum", "title", "indexname", "address", "city", 
                        "county", "countyid", "utm_zone", "utm_east", 
                        "utm_north", "code", "year", "rthl", "loc_desc", "size", 
                        "markertext", "atlas_number"]
        self.UTM_ZONE_ELEMENT = self.columns.index("utm_zone")
        self.UTM_EAST_ELEMENT = self.columns.index("utm_east")
        self.UTM_NORTH_ELEMENT = self.columns.index("utm_north")

        self.orig_c.execute('''SELECT * FROM tx_markets''')

        self.conn = sqlite3.connect(conv_path)
        self.conv_c = self.conn.cursor()

        # Create table
        self.conv_c.execute('''CREATE TABLE tx_markers (markernum TEXT, 
                            title TEXT, indexname TEXT, address TEXT, 
                            city TEXT, county TEXT, countyid TEXT, 
                            lat TEXT, long TEXT, code TEXT, year TEXT, 
                            rthl TEXT, loc_desc TEXT, size TEXT, 
                            marktertext TEXT, atlas_number TEXT)''')

    def next(self):
        row = self.orig_c.fetchone()
        if not row:
            return

        row = list(row)
        zone = row[self.UTM_ZONE_ELEMENT]
        east = row[self.UTM_EAST_ELEMENT]
        north = row[self.UTM_NORTH_ELEMENT]
        if not zone or not east or not north:
            self.write_blank(row)
            return
        zone = int(zone)
        east = int(east)
        north = int(north)
        if north<0 or north > 10000000:
            self.write_blank(row)
            return
        row.pop(self.UTM_ZONE_ELEMENT) # removes unneeded element
        lat, lon = utm.to_latlon(east, north, zone, northern=True)
        row[self.UTM_EAST_ELEMENT] = lat
        row[self.UTM_NORTH_ELEMENT] = lon

        self.conv_c.execute('''INSERT INTO tx_markers VALUES (?,?,?,?,?,?,?,?,
                            ?,?,?,?,?,?,?,?)''', row)

    def write_blank(self, row):
        row.pop(self.UTM_ZONE_ELEMENT) # removes unneeded element
        self.conv_c.execute('''INSERT INTO tx_markers VALUES (?,?,?,?,?,?,?,?,
                            ?,?,?,?,?,?,?,?)''', row)

def main():
    parser = argparse.ArgumentParser(description='Converts a database to lat/long')
    parser.add_argument("address")
    args = parser.parse_args()
    db_conv = db_converter(args.address)
    for i in range(db_conv.ROW_NUM):
        db_conv.next()
    db_conv.conn.commit()

if __name__ == '__main__':
    main()