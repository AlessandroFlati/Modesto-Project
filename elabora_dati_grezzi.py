import pathlib
import re
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests

from eq import calcolaEQ


class Tools:
    def scaricaDST(self, year):
        for n in range(1, 13):
            month = str(n).zfill(2)
            url = 'http://wdc.kugi.kyoto-u.ac.jp/dst_final/' + str(year) + str(month) + '/index.html'
            response = requests.get(url).content.decode('utf-8')
            data = re.findall(r'\d+ +(.*)', response, re.MULTILINE)[1:]
            data = list(map(lambda x: list(map(lambda y: int(y), re.findall(r'(-?\d+)', x))), data))
            df = pd.DataFrame(data, index=range(1, len(data) + 1), columns=range(1, 25))
            df.to_csv('DST' + str(year) + str(month) + '.csv')

    def scaricaKpAp(self, year):
        url = 'http://wdc.kugi.kyoto-u.ac.jp/cgi-bin/kp-cgi'
        response = requests.post(url, {
            'SCent': year - (year % 100),
            'STens': (year % 100) - (year % 10),
            'SYear': year % 10,
            'From': 1,
            'ECent': year - (year % 100),
            'ETens': (year % 100) - (year % 10),
            'EYear': year % 10,
            'To': 12,
            'Email': 'm.lupi@students.uninettunouniversity.net'
        }).content.decode('utf-8')
        data = re.findall(
            r'\d{8} (\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*(\d{1,3})\D*',
            response)
        data = list(map(lambda x: list(map(lambda y: int(y), x)), data))
        df = pd.DataFrame(data, index=range(1, 366),
                          columns=["K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8", "SUM", "a1", "a2", "a3", "a4", "a5",
                                   "a6", "a7", "a8", "Ap"])
        df.to_csv('KPAP' + str(year) + '.csv')

    def scaricaEQ(self, year):
        finalResponse = ""
        for month in range(1, 12):
            url = 'https://earthquake.usgs.gov/fdsnws/event/1/query.csv?starttime=' + str(
                year) + '-' + str(month) + '-01%2000:00:00&endtime=' + str(
                year) + '-' + str(month) + '-31%2023:59:59&minmagnitude=&eventtype=earthquake&orderby=time-asc'
            response = requests.get(url).content.decode('utf-8')
            finalResponse += response
        with open('EQ' + str(year) + '.csv', 'w') as f:
            f.write(finalResponse)

    def elaboraEQ(self, year):
        df = pd.read_csv('EQ' + str(year) + '.csv')
        counter_part = 0
        chunk_size = 1000
        data = []

        for i in range(0, len(df.index)):
            if i % chunk_size == 0:
                print("{:.2f}".format(i * 100 / len(df.index)) + "%")
            if (i + 1) % chunk_size == 0:  # chunk!
                final_df = pd.DataFrame(data, index=range(counter_part * chunk_size + 1,
                                                          counter_part * chunk_size + len(data) + 1),
                                        columns=['year', 'month', 'day', 'hour', 'minute', 'second', 'latitude',
                                                 'longitude',
                                                 'depth', 'mag', 'l_shell', 'eq_b', 'icode'])
                counter_part += 1
                final_df.to_csv('EQ' + str(year) + 'OK.part' + str(counter_part), sep=" ")
                print("I'm chunking! I'm at the " + str(counter_part) + "th part")
                data = []
            try:
                dt = datetime.strptime(df.iloc[i]['time'][:-5], '%Y-%m-%dT%H:%M:%S')
                l_shell, eq_b, icode = calcolaEQ(dt.year, 400, float(df.iloc[i]['latitude']),
                                                 float(df.iloc[i]['longitude']))
                month = dt.strftime('%m')
                day = dt.strftime('%j')
                hour = dt.strftime('%H')
                minute = dt.strftime('%M')
                second = dt.strftime('%S')
                lat = df.iloc[i]['latitude']
                long = df.iloc[i]['longitude']
                depth = df.iloc[i]['depth']
                mag = df.iloc[i]['mag']
                if pd.isna(lat) or pd.isna(long) or pd.isna(depth) or pd.isna(mag):
                    continue

                data.append([year, month, day, hour, minute, second, lat, long, depth, mag, l_shell, eq_b, icode])
            except ValueError:
                print("ValueError with row: " + str(i))
                continue
            except ZeroDivisionError:
                print("ZeroDivisionError with row: " + str(i))
                continue
            except OverflowError:
                print("OverflowError with row: " + str(i))
                continue

        final_df = pd.DataFrame(data,
                                index=range(counter_part * chunk_size + 1, counter_part * chunk_size + len(data) + 1),
                                columns=['year', 'month', 'day', 'hour', 'minute', 'second', 'latitude', 'longitude',
                                         'depth', 'mag', 'l_shell', 'eq_b', 'icode'])
        counter_part += 1
        final_df.to_csv('EQ' + str(year) + 'OK.part' + str(counter_part), sep=" ")

    def elaboraCR(self, year, doy1, doy2):
        clock_time = time.clock()

        df2 = self.parsePSSET(year, doy1, doy2)
        df1 = self.parseRSSET(year, doy1, doy2)

        year = str(year)
        doy1 = str(doy1).zfill(3)
        doy2 = str(doy2).zfill(3)

        idx = df1.index.intersection(df2.index)

        df1 = df1.loc[idx]
        df2 = df2.loc[idx]

        df = pd.DataFrame()

        df = df.assign(ELO=df1['ELO'], EHI=df1['EHI'], PLO=df1['PLO'], PHI=df1['PHI'])
        df = df.assign(SAA_Flag=df2['SAA_Flag'])  # , SAAMy=[9999] * L)
        df = df.assign(GEO_Lat=df2['GEO_Lat'], GEO_Long=df2['GEO_Long'], Altitude=df2['Altitude'],
                       L_Shell=df2['L_Shell'])
        df = df.assign(Pitch=df2['Pitch'], Att_Flag=df2['Att_Flag'])
        df.index.name = 'Timestamp'

        df.to_csv('./MATLAB/cr' + year + '_' + doy1 + '_' + doy2 + '.csv', sep=' ')

        dt = int(time.clock() - clock_time)
        secs = dt % 60
        mins = int((dt - secs) / 60) % 60
        print("Done in {:d} minute{} and {:d} second{}".format(mins, 's' if mins != 1 else '', secs,
                                                               's' if secs != 1 else ''))

    def createTimestamps(self, df):
        timestamps = []
        for i in range(0, len(df.index)):
            v = df.iloc[i]
            second = int(v['Sec_of_day'] % 60)
            minute = int((v['Sec_of_day'] - second) / 60 % 60)
            hour = int(((v['Sec_of_day'] - second) / 60 - minute) / 60 % 24)
            date = datetime.strptime(
                str(int(v['Year'])) + str(int(v['Day-of-year'])).zfill(3) + str(hour).zfill(2) + str(
                    minute).zfill(2) + str(second).zfill(2), '%Y%j%H%M%S')
            timestamps.append(date.timestamp())
        return timestamps

    def parseRSSET(self, year, doy1, doy2):
        df = pd.DataFrame()
        list_of_filenames = self.get_list_of_SSET(year, doy1, doy2)
        for filename in list_of_filenames:
            fileRSSET = 'RSSetFiles/RSSet' + filename
            with open(fileRSSET, 'r') as f:
                print(fileRSSET)
                csv = self.parse_SSET(f, 6, year, doy1, doy2)
                if not csv.empty:
                    csv = csv[['ELO', 'EHI', 'PLO', 'PHI']]
                    df = df.append(csv)
        return df

    def parsePSSET(self, year, doy1, doy2):
        df = pd.DataFrame()
        list_of_filenames = self.get_list_of_SSET(year, doy1, doy2)
        for filename in list_of_filenames:
            filePSSET = 'PSSetFiles/PSSet' + filename
            with open(filePSSET, 'r') as f:
                print(filePSSET)
                csv = self.parse_SSET(f, 4, year, doy1, doy2)
                if not csv.empty:
                    csv = csv[['GEO_Long', 'GEO_Lat', 'Altitude', 'L_Shell', 'Pitch', 'Att_Flag', 'SAA_Flag']]
                    csv = csv[csv['Att_Flag'].isin([0, 1, 100, 101])]
                    df = df.append(csv)
        return df

    def parse_SSET(self, f, lines_to_skip, year, doy1, doy2):
        for k in range(0, lines_to_skip):
            f.readline()
        headers, data = f.read().split('BEGIN DATA')
        headers = re.findall(r':\s(\S+)[\s:]-?', headers)
        headers = headers[:-1]
        columns = []
        for header in headers:
            if 'Dipole' in header:  # PSSET HEADER IS MALFORMED!
                columns.append(header[:-1] + 'X')
                columns.append(header[:-1] + 'Y')
                columns.append(header[:-1] + 'Z')
                continue

            if ',' in header:
                if '_' in header:
                    pre, post = header.split('_')
                    pre += '_'
                else:
                    pre = ''
                    post = header
                post = post.split(',')
                for p in post:
                    col = pre + str(p)
                    c = 1
                    while col in columns:
                        col += str(c)
                        c += 1
                    columns.append(col)
            else:
                c = 1
                while header in columns:
                    header += str(c)
                    c += 1
                columns.append(header)

        data = StringIO(data)
        csv = pd.read_csv(data, sep=' ', header=None, index_col=False, names=columns)
        # todo: check years
        csv = csv[csv['Day-of-year'].isin(range(doy1, doy2))]
        csv.index = self.createTimestamps(csv[['Year', 'Day-of-year', 'Sec_of_day']])
        return csv

    def get_list_of_SSET(self, year, doy1, doy2):
        list_of_SSET = []
        # todo: check years
        for i in range(0, doy2):
            for j in range(i, 367):
                if range(max(doy1, i), min(doy2, j) + 1):
                    common_part = '_6sec_' + str(year) + str(i).zfill(3) + '_' + str(year) + str(j).zfill(3) + '.txt'
                    if pathlib.Path('PSSetFiles/PSSet' + common_part).exists() and pathlib.Path(
                                    'RSSetFiles/RSSet' + common_part).exists():
                        list_of_SSET.append(common_part)
        return list_of_SSET


t = Tools()
# t.scaricaDST(1993)
# t.scaricaKpAp(1993)
# t.scaricaEQ(1993)
# t.elaboraEQ(1993)
# t.elaboraCR(1993, 15, 16)
t.elaboraCR(1993, 15, 46)
# t.elaboraCR(1993, 13, 363)
# t.elaboraCR(1999, 9, 359)

# def calcolaiSAAMy(self, fileCR, integraStep = "6"):
# f1, f2, r1, o1
# c
# if fileCR.size() > 0:
#     fortran_input("external/external_input_cr.dat")
#     for nf in range(0, len(fileCR)):
#         f1 = utils::leggiFile(fileCR[nf] + integraStep)
#         anno = int(f1[0].substr(6, 2).c_str())
#         if anno >= 90:
#             anno = 1900 + anno
#         else:
#             anno = 2000 + anno
#         fortran_input + str(anno) + str()". 1.8" + str(endl)
#         c = 0
#         while c < f1.size():
#             r1 = f1[c].split()
#             fortran_input + str(r1[15]) + str()" " + str(r1[13]) + str()" " + str(r1[14]) + str()" " + str(r1[18]) + str(endl)
#             c++
#         fortran_input.close()
#         system("cd external and ./calcolaiSAAMy")
#         o1 = utils::leggiFile("external/external_output_cr.dat")
#         if o1.size() == f1.size():
#             c = 0
#             while c < o1.size():
#                 int iSAAMy = int(o1[c].c_str())
#                 r1 = f1[c].split()
#                 r1[11] = iSAAMy
#                 f2.push_back(utils::implode(r1, " "))

# def integra30sec(self, file6sec):
# ofs, f1, rLUp = 1.8, c, data
# rLat, rLong, rAlt, rLShell, rPitch
# iPEloT, iPEhiT, iPPloT, iPPhiT, iPEloCR, iPEhiCR, iPPloCR, iPPhiCR, att, iSAA, iSAAMy
#
# for nf in range(0,len(file6sec)):
#     f1 = utils::leggiFile(file6sec[nf] + "6")
#     c = 0
#     r1, r2, r3, r4, r5
#     ofs.open(file6sec[nf] + "30")
#     while c < f1.size() - 4:
#         r1 = f1[c].split()
#         r2 = f1[c + 1].split()
#         r3 = f1[c + 2].split()
#         r4 = f1[c + 3].split()
#         r5 = f1[c + 4].split()
#         s1, s2, s3, s4, s5
#         s1 = int(r1[1].substr(6, 2).c_str())
#         s2 = int(r2[1].substr(6, 2).c_str())
#         s3 = int(r3[1].substr(6, 2).c_str())
#         s4 = int(r4[1].substr(6, 2).c_str())
#         s5 = int(r5[1].substr(6, 2).c_str())
#         data = r1[0] + " " + r1[1]
#         iPEloCR = int(r1[2].c_str()) + int(r2[2].c_str()) + int(r3[2].c_str()) + int(r4[2].c_str()) + int(r5[2].c_str())
#         iPEloT = int(r1[3].c_str()) + int(r2[3].c_str()) + int(r3[3].c_str()) + int(r4[3].c_str()) + int(r5[3].c_str())
#         iPEhiCR = int(r1[4].c_str()) + int(r2[4].c_str()) + int(r3[4].c_str()) + int(r4[4].c_str()) + int(r5[4].c_str())
#         iPEhiT = int(r1[5].c_str()) + int(r2[5].c_str()) + int(r3[5].c_str()) + int(r4[5].c_str()) + int(r5[5].c_str())
#         iPPloCR = int(r1[6].c_str()) + int(r2[6].c_str()) + int(r3[6].c_str()) + int(r4[6].c_str()) + int(r5[6].c_str())
#         iPPloT = int(r1[7].c_str()) + int(r2[7].c_str()) + int(r3[7].c_str()) + int(r4[7].c_str()) + int(r5[7].c_str())
#         iPPhiCR = int(r1[8].c_str()) + int(r2[8].c_str()) + int(r3[8].c_str()) + int(r4[8].c_str()) + int(r5[8].c_str())
#         iPPhiT = int(r1[9].c_str()) + int(r2[9].c_str()) + int(r3[9].c_str()) + int(r4[9].c_str()) + int(r5[9].c_str())
#         iSAA = int(r1[11].c_str())
#         iSAAMy = int(r1[12].c_str())
#         rAlt = float(r1[15].c_str())
#         rLat = float(r1[13].c_str())
#         rLong = float(r1[14].c_str())
#         rLShell = float(r1[16].c_str())
#         rPitch = float(r1[19].c_str())
#         ofs + str(data) + str()" " + str(iPEloCR) + str()" " + str(iPEloT) + str()" " + str(iPEhiCR) + str()" " + str(iPEhiT) + str()" " + str(iPPloCR) + str()" " + str(iPPloT) + str()" " + str(iPPhiCR) + str()" " + str(iPPhiT) + str()" " + str(0) + str()" " + str(iSAA) + str()" " + str(iSAAMy) + str()" " + str(rLat) + str()" " + str(rLong) + str()" " + str(rAlt) + str()" " + str(rLShell) + str()" " + str(0) + str()" " + str(0.) + str()" " + str(rPitch) + str(endl)
#         c = c + 5
#     ofs.close()
