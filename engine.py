import os
import openpyxl as ox
import xlwings as xw


def ox_load_workbook(filepath):
    _path = '/'.join(filepath.split('/')[:-1])
    _name = filepath.split('/')[-1]

    os.chdir(_path)

    wb = ox.load_workbook(_name)
    ws = wb['Sheet1']
    ws['A1'] = 'test'
    wb.save(_name)

    return True


def xw_load_workbooks(filepath):
    return xw.Book(filepath)


if __name__ == '__main__':
    test_file = 'D:/Localhome/sekim/OneDrive - ZF Friedrichshafen AG/Desktop/NPV concept B.xlsx'
    # ox_load_workbook(test_file)

    print(xw_load_workbooks(test_file))
