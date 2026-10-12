# NYC Taxi Data Warehouse - SSIS

Du an ETL du lieu NYC TLC cho bon nhom: Yellow Taxi, Green Taxi, FHV va High Volume FHV.

## Extract theo nam va thang

Cai thu vien mot lan:

```powershell
py -m pip install -r NYCTaxi_DW\scripts\requirements.txt
```

Tai va convert ca bon bo du lieu cua thang 01/2026:

```powershell
py NYCTaxi_DW\scripts\extract_nyc_taxi.py --year 2026 --month 1 --data-root "D:\Kho_Du_lieu\NYCTaxi"
```

Chi xu ly Yellow va Green:

```powershell
py NYCTaxi_DW\scripts\extract_nyc_taxi.py --year 2026 --month 1 --datasets yellow green
```

Quy tac chay lai an toan:

- Parquet da ton tai va co chu ky `PAR1` hop le se duoc bo qua.
- CSV da ton tai va khong rong se duoc bo qua.
- File dang tai/chuyen doi mang duoi `.part`; chi doi thanh ten chinh thuc khi hoan tat.
- Neu lan chay truoc bi dung giua chung, lan sau se xoa `.part` cu va xu ly lai file do.

Cau truc ket qua:

```text
D:\Kho_Du_lieu\NYCTaxi
|-- raw\yellow\year=2026\month=01\yellow_tripdata_2026-01.parquet
|-- raw\green\year=2026\month=01\green_tripdata_2026-01.parquet
|-- raw\fhv\year=2026\month=01\fhv_tripdata_2026-01.parquet
|-- raw\fhvhv\year=2026\month=01\fhvhv_tripdata_2026-01.parquet
`-- csv\... (giu cung cau truc, duoi .csv)
```

## Goi tu SSIS

Trong `PKG_01_Extract_Stage.dtsx`, cau hinh `Execute Process Task` dau tien:

- Executable: duong dan `python.exe` tren may chay SSIS.
- WorkingDirectory: thu muc `NYCTaxi_DW\scripts` cua du an.
- Arguments:

```text
extract_nyc_taxi.py --year 2026 --month 1 --data-root "D:\Kho_Du_lieu\NYCTaxi"
```

Khi dua len SSIS Catalog, nen tao ba Project Parameter `ExtractYear`, `ExtractMonth`, `DataRoot`, roi dung Expression cho thuoc tinh `Arguments`. Vi script tong da thuc hien ca download va convert, hay disable tac vu convert cu de tranh chay hai lan.

> Luu y: buoc Extract trong tai lieu nay ket thuc tai CSV. Cac Data Flow nap CSV vao staging van la buoc Load-to-Staging va can cau hinh source/destination rieng cho tung loai du lieu.
