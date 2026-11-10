"""Offline baseline for the upstream Python backend; no authoring task solutions."""
from io import BytesIO
from decimal import Decimal
from fastavro import _read_py, _write_py, _schema_py

schema = {'type':'record','name':'Row','fields':[
    {'name':'id','type':'long'},
    {'name':'label','type':['null','string']},
    {'name':'values','type':{'type':'array','items':'int'}},
    {'name':'amount','type':{'type':'bytes','logicalType':'decimal','precision':8,'scale':2}}
]}
rows=[{'id':i,'label':None if i%2 else '中文','values':[i,-i], 'amount':Decimal('12.30')} for i in range(12)]
checks=0
for codec in ('null','deflate','bzip2','xz'):
    output=BytesIO()
    _write_py.writer(output,schema,rows,codec=codec,sync_interval=40)
    raw=output.getvalue()
    assert list(_read_py.reader(BytesIO(raw))) == rows
    blocks=list(_read_py.block_reader(BytesIO(raw)))
    assert len(blocks)>1
    assert [r for block in blocks for r in block] == rows
    checks+=2
reader_schema={'type':'record','name':'Row','fields':[
    {'name':'new_id','aliases':['id'],'type':'long'},
    {'name':'extra','type':'string','default':'x'}]}
assert list(_read_py.reader(BytesIO(raw),reader_schema))==[{'new_id':i,'extra':'x'} for i in range(12)]
checks+=1
for record in rows:
    out=BytesIO();_write_py.schemaless_writer(out,schema,record);out.seek(0)
    assert _read_py.schemaless_reader(out,schema)==record
    checks+=1
print(f'{checks} Python-backend binary roundtrip/block/schema-resolution checks passed')
