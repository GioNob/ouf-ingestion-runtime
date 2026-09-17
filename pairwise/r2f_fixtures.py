"""Deterministic 2D Shapefile input, generated without a second GIS runtime."""
import pathlib, struct, zipfile

def shapefile(directory):
    def header(size):
        b=bytearray(size)
        struct.pack_into('>i',b,0,9994);struct.pack_into('>i',b,24,size//2)
        struct.pack_into('<ii',b,28,1000,1)
        struct.pack_into('<dddd',b,36,13.77,45.65,13.77,45.65)
        return b
    shp=header(128);struct.pack_into('>ii',shp,100,1,10);struct.pack_into('<idd',shp,108,1,13.77,45.65)
    shx=header(108);struct.pack_into('>ii',shx,100,50,10)
    fields=[('ID','S-001'),('CODE','001'),('NAME','Asset Roma'),('DISTRICT','Centro')]
    width=24;hsize=32+32*len(fields)+1;rsize=1+width*len(fields)
    dbf=bytearray(hsize+rsize+1);dbf[0]=3
    struct.pack_into('<IHH',dbf,4,1,hsize,rsize)
    for i,(name,value) in enumerate(fields):
        start=32+32*i;dbf[start:start+len(name)]=name.encode('ascii');dbf[start+11]=ord('C');dbf[start+16]=width
        start=hsize+1+width*i;dbf[start:start+width]=value.encode('ascii').ljust(width,b' ')
    dbf[hsize-1]=13;dbf[hsize]=32;dbf[-1]=26
    wkt='GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'
    directory=pathlib.Path(directory);directory.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(directory/'assets.zip','w',zipfile.ZIP_DEFLATED) as z:
        for ext,content in [('shp',shp),('shx',shx),('dbf',dbf),('prj',wkt.encode()),('cpg',b'UTF-8')]:
            info=zipfile.ZipInfo('assets.'+ext,date_time=(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,content)

def geopackage(directory):
    """Same mapped attributes and point as the Shapefile, with an excluded physical row id."""
    import sqlite3
    target=pathlib.Path(directory)/'assets.gpkg'
    target.unlink(missing_ok=True)
    with sqlite3.connect(target) as db:
        db.executescript('''
        PRAGMA application_id=1196444487;
        PRAGMA user_version=10300;
        CREATE TABLE gpkg_spatial_ref_sys(srs_name TEXT NOT NULL,srs_id INTEGER PRIMARY KEY,organization TEXT NOT NULL,organization_coordsys_id INTEGER NOT NULL,definition TEXT NOT NULL,description TEXT);
        INSERT INTO gpkg_spatial_ref_sys VALUES('WGS 84',4326,'EPSG',4326,'EPSG:4326','Fixture');
        CREATE TABLE gpkg_contents(table_name TEXT PRIMARY KEY,data_type TEXT NOT NULL,identifier TEXT,description TEXT DEFAULT '',last_change DATETIME,min_x DOUBLE,min_y DOUBLE,max_x DOUBLE,max_y DOUBLE,srs_id INTEGER);
        INSERT INTO gpkg_contents VALUES('assets','features','assets','Equivalent Shapefile dataset','2026-09-17T00:00:00Z',13.77,45.65,13.77,45.65,4326);
        CREATE TABLE gpkg_geometry_columns(table_name TEXT NOT NULL,column_name TEXT NOT NULL,geometry_type_name TEXT NOT NULL,srs_id INTEGER NOT NULL,z INTEGER NOT NULL,m INTEGER NOT NULL,PRIMARY KEY(table_name,column_name));
        INSERT INTO gpkg_geometry_columns VALUES('assets','geom','POINT',4326,0,0);
        CREATE TABLE assets(fid INTEGER PRIMARY KEY,ID TEXT,CODE TEXT,NAME TEXT,DISTRICT TEXT,geom BLOB);
        ''')
        geometry=b'GP'+bytes([0,1])+struct.pack('<i',4326)+struct.pack('<bIdd',1,1,13.77,45.65)
        db.execute('INSERT INTO assets VALUES(1,?,?,?,?,?)',('S-001','001','Asset Roma','Centro',geometry))
