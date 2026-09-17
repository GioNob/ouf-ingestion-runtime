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
