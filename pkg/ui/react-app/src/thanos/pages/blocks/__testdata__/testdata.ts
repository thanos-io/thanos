import { BlockListProps } from '../Blocks';

export const sampleAPIResponse: { status: string; data: BlockListProps } = {
  data: {
    blocks: [
      {
        meta: {
          ulid: '01EKB475GXXJ1AA3SGSNJ0JNF6',
          minTime: 1601321100000,
          maxTime: 1601321400000,
          stats: {
            numSamples: 2182540,
            numSeries: 2152,
            numChunks: 14257
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB475GXXJ1AA3SGSNJ0JNF6'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 257901,
          chunkSize: 4522807
        }
      },
      {
        meta: {
          ulid: '01EKB3YA1PNKA7HPJ40A3NXGGG',
          minTime: 1601164800000,
          maxTime: 1601177700000,
          stats: {
            numSamples: 281316024,
            numSeries: 2189,
            numChunks: 2336001
          },
          compaction: {
            level: 3,
            sources: [
              '01EK6E5KF4GSVFHEHGF60M0NA9',
              '01EK6EERE8SV341EYXQ7V6D0VQ',
              '01EK6EQXD8QX649CCR9WR507CR',
              '01EK6F12C55QCMSJB423JBJS7V',
              '01EK6FA7B63DEAC9AV4SC700E9',
              '01EK6FKCA77QKR3Y8EP50HYMYE',
              '01EK6FWH98HP8TV8WHXGWNGBEK',
              '01EK6G5PA8E5K10GFZ7W95DFPZ',
              '01EK6GEV75HNS9FE5BZ0XYTQ9T',
              '01EK6GR06WC8JT6S0JJRMXR98V',
              '01EK6H155957839FSHA291RYDK',
              '01EK6HAA46C9XPH14WWDYJNBTC',
              '01EK6HKF3ED8EYDE1KMRZCBSA6',
              '01EK6HWM27C2JTM28EPCMPM4VP',
              '01EK6J5S195PMQDPGTRVAKQMYS',
              '01EK6JEY4XMMJXWJPRFBM19FYS',
              '01EK6JR2Z8YXS975E1Q0XDE8YY',
              '01EK6K17YFKR6PRQJ9H7VDTCQ7',
              '01EK6KACY4AB3X8GN5EBWTZST7',
              '01EK6KKHWXHFRBZP4VQZ4D5PZ3',
              '01EK6KWPVV3E1EH7YSNSBJX1G0',
              '01EK6M5VTFRH6FZPCYRKKHE1Z7',
              '01EK6MF0S5F8EQ0Z2B2WADBBZ0',
              '01EK6MR5RMJXCP1DQ3Y5KSD9WK',
              '01EK6N1AQA9TE267GYN46JYWGB',
              '01EK6NAFPW7HAMWZ4XQY8D991A',
              '01EK6NKMNX8WS5Y3Z08SAB82K9',
              '01EK6NWSMAHECGM37JZ5P4CJZQ',
              '01EK6P5YKBYEQCD8EPVVEESJZM',
              '01EK6PF3JWFE8J2DJCCA4PYHFV',
              '01EK6PR8H5ENE65CR629BHSDB3',
              '01EK6Q1DGAFXE5GBQ7F1V6SBFH',
              '01EK6QAJF3NN9XQ745GRVRNA04',
              '01EK6QKQEVYJQ0ANSC0P63JZNG',
              '01EK6QWWDR5T3BKYNS2EX04TR6',
              '01EK6R61C4KP0AS3RVCV07ZCAC',
              '01EK6RF6B7N9EG059NRDGTA3D1',
              '01EK6RRBAZRQ13XZ7EHY7RPY1G',
              '01EK6S1GAEM89PZ92852EQQ6N6',
              '01EK6SAN9FBBK1QY2SXWGGQZ1N',
              '01EK6SKT86ZQ8JS4D8Y7P2QF6G',
              '01EK6SWZ6W4FYX3JW6JQ0W39JH',
              '01EK6T645SR4K1FHBJNQ2ZSJQV'
            ],
            parents: [
              {
                ulid: '01EK6PT81PPY7FT2ES77BWKQC1',
                minTime: 1601164800000,
                maxTime: 1601172000000
              },
              {
                ulid: '01EKB3V7EMDH208GVQEATES139',
                minTime: 1601172000000,
                maxTime: 1601177700000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 11425368,
          chunkSize: 302636670
        }
      },
      {
        meta: {
          ulid: '01EKB5GQY8RA93N4GKAT17HVJS',
          minTime: 1601323500000,
          maxTime: 1601323800000,
          stats: {
            numSamples: 6471467,
            numSeries: 2187,
            numChunks: 53566
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB5GQY8RA93N4GKAT17HVJS'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 465358,
          chunkSize: 10760675
        }
      },
      {
        meta: {
          ulid: '01EKB47MWNGKHAG4PGMY6W2YWB',
          minTime: 1601321400000,
          maxTime: 1601321700000,
          stats: {
            numSamples: 872113,
            numSeries: 2152,
            numChunks: 8608
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB47MWNGKHAG4PGMY6W2YWB'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 226397,
          chunkSize: 1374011
        }
      },
      {
        meta: {
          ulid: '01EKB47KP2TPZZ3DJ3EWVPNA6Q',
          minTime: 1601321400000,
          maxTime: 1601321700000,
          stats: {
            numSamples: 871560,
            numSeries: 2152,
            numChunks: 8608
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB47KP2TPZZ3DJ3EWVPNA6Q'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 226397,
          chunkSize: 1517839
        }
      },
      {
        meta: {
          ulid: '01EKB4C4237CS74BZ4GZZ89GWB',
          minTime: 1601322300000,
          maxTime: 1601322600000,
          stats: {
            numSamples: 6331268,
            numSeries: 2187,
            numChunks: 50840
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4C4237CS74BZ4GZZ89GWB'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 456925,
          chunkSize: 14453414
        }
      },
      {
        meta: {
          ulid: '01EK97E1PY37JYP8096VDM6P4W',
          minTime: 1601258400000,
          maxTime: 1601258700000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK97E1PY37JYP8096VDM6P4W'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 108942,
          chunkSize: 1138383
        }
      },
      {
        meta: {
          ulid: '01EKB4N91Z0D4M83MFVRNESSMZ',
          minTime: 1601322600000,
          maxTime: 1601322900000,
          stats: {
            numSamples: 6400439,
            numSeries: 2187,
            numChunks: 52491
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4N91Z0D4M83MFVRNESSMZ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 463393,
          chunkSize: 11798780
        }
      },
      {
        meta: {
          ulid: '01EK994ZGZS6MEHD9GM5JEEQFA',
          minTime: 1601260200000,
          maxTime: 1601260500000,
          stats: {
            numSamples: 1377976,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK994ZGZS6MEHD9GM5JEEQFA'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 152007,
          chunkSize: 1213260
        }
      },
      {
        meta: {
          ulid: '01EK9A0EE6KN3GT16T5M18GVXP',
          minTime: 1601261100000,
          maxTime: 1601261400000,
          stats: {
            numSamples: 1379928,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9A0EE6KN3GT16T5M18GVXP'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155959,
          chunkSize: 1331386
        }
      },
      {
        meta: {
          ulid: '01EK6G5RNS3Q1R9VK4NX02JE1R',
          minTime: 1600992000000,
          maxTime: 1601164800000,
          stats: {
            numSamples: 183464,
            numSeries: 2512,
            numChunks: 11204
          },
          compaction: {
            level: 4,
            sources: [
              '01EK19C5F5VENWMFM7FYRKPF05',
              '01EK19NADQ72YVT7PWYC5E0Z6C',
              '01EK19YFDKC7PXKSEWC4BKZM35',
              '01EK1A7MBY5N1JXF1D0MM6KGWQ',
              '01EK1AGSATBBPCR80XD3KCG3JT',
              '01EK1ASYAKS1WH3QNHQET44595',
              '01EK1B339B02PHCT6M004E9W0V',
              '01EK1BC87TX49GTW6GT5F3105R',
              '01EK1BSB7C22M57AER7D3P8QKN',
              '01EK1BYJ60R79KBDP5XVEMTF37',
              '01EK1C7Q5BA8FRPC1GPCQKF3EA',
              '01EK1CGW4FE3YMQW77AMBFXE86',
              '01EK1CT13547VGBEEFAZVKVPH3',
              '01EK1D362MKN4BARR8JBV98NC4',
              '01EK1DCB1QA35S1EHDQAVGSEEE',
              '01EK1DNG0HB7D04QN72V7YVKS3',
              '01EK1DYMZA152W0DAKN2YXABBG',
              '01EK1E7SY32XX4MFYNW2QVHHRM',
              '01EK1EGYXBZH37EN5HM0JAKASE',
              '01EK1ET3WC022F2JHST33X8H3J',
              '01EK1F38V6Y2KGFZDBGPPSDRCD',
              '01EK1FCDTAKBWWAD93JQN5D35M',
              '01EK1FNJS6T4NZRH0BYRBJRNDJ',
              '01EK1FYQR703F3CARPXXHF6B3R',
              '01EK1G7WQ8QM4GBW2BJCQ89MJ1',
              '01EK1GH1PCCPRF1V5KWK722B6T',
              '01EK1GT6N5ZV7GBVZEGAS4R8JK',
              '01EK1H3BM6W973VRTPVE46DKVM',
              '01EK5VJGG2TFP6DVCC1WRFFGRD',
              '01EK5VVNFHK6SKW9P4Q3151NGJ',
              '01EK5W4TERCAH70TVTE9B41RVR',
              '01EK5WDZD1N0QFYR8JN4SQKJZT',
              '01EK5WQ4C1DSF3J11H3THF0QJS',
              '01EK5X09B2RMTW7PE2X4E4XQT7',
              '01EK5X9EA87QJTPM3J0WNFZS5X',
              '01EK5XJK986S292D4J0QZBSWGF',
              '01EK5XVR8GV6EJEEMNBGH666JH',
              '01EK5Y4X71K5SMG9A8XGWE1SST',
              '01EK5YE26CW66Z3YSPDDQNDZRR',
              '01EK5YQ752XSQTQWXM5CFVFVHT',
              '01EK5Z0C4A3EB1NEK4FGPN6EHX',
              '01EK5Z9H30RWXJFFAHSY2DZRTQ',
              '01EK5ZJP2WKCYYYXPQ6PGKTRGR',
              '01EK5ZVV11R47T9A8NCT5VKDKQ',
              '01EK605005PRHY28N0S7E87WTM',
              '01EK60E50Z0W7YQHAWPCZWRSBM',
              '01EK60Q9Y5Y0RG5QJYBMZVWK7G',
              '01EK610EX0TQAF8KFCN1G4GEBG',
              '01EK619KX1HFY96Q32MSSEXNAP',
              '01EK61JRV1BF17BX79HZ87R7XG',
              '01EK61VXT235H6G1YSAWCXV36N',
              '01EK6252S2AXDNZWNTAYM4KRSM',
              '01EK62E7S8SVPCGEPDYJYMMHYT',
              '01EK62QCQGJADX3NTZT1BR4QMA',
              '01EK630HP8GYQ3ZYNXCKZQ8YWE',
              '01EK639PNM8ZB5HQ0F34R7ZEQM',
              '01EK63JVM1YCTVM9609FHK3CV4',
              '01EK670KC1BG42PW4BK34FFP1W',
              '01EK670S3BE4X6KJACJWAJYNTS',
              '01EK679W8GPJ0X6YEY8BWCZAJN',
              '01EK67K15ZE9WYSGHDVBDF26FD',
              '01EK67W64T6MSYPEH29A2YG3NX',
              '01EK685B465HQZG143BXV6FPGN',
              '01EK68EG2WBWVXTWAMAED9WETH',
              '01EK68QN21XABAQZN743NC1HTC',
              '01EK690T11EE2RMKKPS2657QNC',
              '01EK699YZY7CEHJZHMWYQCVV7E',
              '01EK69K3YTG75ZHVKMGZ7TE4RT',
              '01EK69W8Y07230G1WGEYJ8DS45',
              '01EK6A5DX78ZQXKG0KQ7C8J75C',
              '01EK6AEJVVHPSHP5R9GCER1YGR',
              '01EK6AQQV7GXSFH6E8ETC9GXVK',
              '01EK6B0WT1BVGD7VTD1WMTVAE1',
              '01EK6BA1S8AG3KY124BFY7PN1S',
              '01EK6BK6R4CRFRNRSNF3H7Q308',
              '01EK6BWBPWPR0M7BG5GKKRA1V2',
              '01EK6C5GP4P37D6AFDXDQBB1T9',
              '01EK6CENNCFFK8BPGF3FPMTFKV',
              '01EK6CQTKXX1M5N4NXW9MWR18E',
              '01EK6D0ZK5BANSQGRXAZH87D1E',
              '01EK6DA4J56P5Z7EPTD40FGBPS',
              '01EK6DK9GY9A0NPMEVNB3Z33DM',
              '01EK6DWEFYWE17FTJRZ9V2HS9K'
            ],
            parents: [
              {
                ulid: '01EK5XNEG9MH36E44M0GHQR62T',
                minTime: 1600992000000,
                maxTime: 1601000400000
              },
              {
                ulid: '01EK62F4XTZKM1P2G48S4BJSX7',
                minTime: 1601145300000,
                maxTime: 1601150400000
              },
              {
                ulid: '01EK69D7YKZX3N4S7R3VY52A0D',
                minTime: 1601150400000,
                maxTime: 1601154600000
              },
              {
                ulid: '01EK6FY0RNDXP8GD8QJ3EDG62Y',
                minTime: 1601157600000,
                maxTime: 1601164800000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 300000
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 312710,
          chunkSize: 2845964
        }
      },
      {
        meta: {
          ulid: '01EK9AJRC5NHM6RFGFGAVYA6VC',
          minTime: 1601261700000,
          maxTime: 1601262000000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9AJRC5NHM6RFGFGAVYA6VC'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 113342,
          chunkSize: 1273770
        }
      },
      {
        meta: {
          ulid: '01EK98JNK5W2R79WH2NPW9DXFJ',
          minTime: 1601259600000,
          maxTime: 1601259900000,
          stats: {
            numSamples: 1377976,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK98JNK5W2R79WH2NPW9DXFJ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 151991,
          chunkSize: 1190230
        }
      },
      {
        meta: {
          ulid: '01EK9AJRC1FV2ZEZA0PCJSNN1G',
          minTime: 1601261700000,
          maxTime: 1601262000000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9AJRC1FV2ZEZA0PCJSNN1G'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 109134,
          chunkSize: 1180992
        }
      },
      {
        meta: {
          ulid: '01EK97E1Q161RNZPW8J304XW2X',
          minTime: 1601258400000,
          maxTime: 1601258700000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK97E1Q161RNZPW8J304XW2X'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 113294,
          chunkSize: 1311294
        }
      },
      {
        meta: {
          ulid: '01EKB4YE03NCFDQP98AM3RVJ61',
          minTime: 1601322900000,
          maxTime: 1601323200000,
          stats: {
            numSamples: 6534940,
            numSeries: 2187,
            numChunks: 54675
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4YE03NCFDQP98AM3RVJ61'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 466901,
          chunkSize: 10131650
        }
      },
      {
        meta: {
          ulid: '01EK989GM0G4JAN8MVVD7XWTMY',
          minTime: 1601259300000,
          maxTime: 1601259600000,
          stats: {
            numSamples: 1379930,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK989GM0G4JAN8MVVD7XWTMY'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 154079,
          chunkSize: 1237237
        }
      },
      {
        meta: {
          ulid: '01EKB480K6B6M04S7X3THH2KJ9',
          minTime: 1601322000000,
          maxTime: 1601322300000,
          stats: {
            numSamples: 6332648,
            numSeries: 2179,
            numChunks: 50397
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB480K6B6M04S7X3THH2KJ9'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 453230,
          chunkSize: 13788091
        }
      },
      {
        meta: {
          ulid: '01EKB5SWXBD41GTX70HZ0ZSNYB',
          minTime: 1601323800000,
          maxTime: 1601324100000,
          stats: {
            numSamples: 6490430,
            numSeries: 2187,
            numChunks: 53315
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB5SWXBD41GTX70HZ0ZSNYB'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 463798,
          chunkSize: 12687819
        }
      },
      {
        meta: {
          ulid: '01EKB57JZ0FEFHGJEKXSJXSR0W',
          minTime: 1601323200000,
          maxTime: 1601323500000,
          stats: {
            numSamples: 6201149,
            numSeries: 2189,
            numChunks: 51451
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB57JZ0FEFHGJEKXSJXSR0W'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 458998,
          chunkSize: 11312548
        }
      },
      {
        meta: {
          ulid: '01EKB57JYQAFW5VEF3PF3NR4H8',
          minTime: 1601323200000,
          maxTime: 1601323500000,
          stats: {
            numSamples: 6182827,
            numSeries: 2189,
            numChunks: 50829
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB57JYQAFW5VEF3PF3NR4H8'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 453614,
          chunkSize: 11428401
        }
      },
      {
        meta: {
          ulid: '01EK980BN2H98SEQ566EY24GBB',
          minTime: 1601259000000,
          maxTime: 1601259300000,
          stats: {
            numSamples: 1377976,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK980BN2H98SEQ566EY24GBB'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 151991,
          chunkSize: 1259640
        }
      },
      {
        meta: {
          ulid: '01EK9A0EDZAWBZQ6BRJS7MSXWX',
          minTime: 1601261100000,
          maxTime: 1601261400000,
          stats: {
            numSamples: 1377976,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9A0EDZAWBZQ6BRJS7MSXWX'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 148071,
          chunkSize: 1188788
        }
      },
      {
        meta: {
          ulid: '01EK99Q9F3YDN93NAX8DV17Z20',
          minTime: 1601260800000,
          maxTime: 1601261100000,
          stats: {
            numSamples: 1378952,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK99Q9F3YDN93NAX8DV17Z20'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 154039,
          chunkSize: 1211350
        }
      },
      {
        meta: {
          ulid: '01EKB62K6M57STV6AJJKJA1K96',
          minTime: 1601254800000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 34174833,
            numSeries: 948,
            numChunks: 294372
          },
          compaction: {
            level: 3,
            sources: [
              '01EK94062YPNMPTY7EXWXEHSKN',
              '01EK949B1Y77YMEJY4VHX2X58R',
              '01EK94JG1076SSH65BHRBM3GAJ',
              '01EK94VMZT4YRJ6QT4DCMX7E7W',
              '01EK954SZ0ME5W5R2FDM1SKB31',
              '01EK95DYXZQVMWXED5W8JKGCEB',
              '01EK95Q3WVZ6GQZAZ3EBMMFP6G',
              '01EK9608VYTEPY5W1WYJ2Z7X89',
              '01EK969DV0Q0BGZBMZJ10SYMXS',
              '01EK96JJT07BC6A2FCPSZ8QBXC',
              '01EK96VQRZQKW0RMND515HTC9Z',
              '01EK974WR0H82P52D7Z13FKW19',
              '01EK97E1PY37JYP8096VDM6P4W',
              '01EK97Q6P1ZZE791VSW9YMRQN3',
              '01EK980BN2H98SEQ566EY24GBB',
              '01EK989GM0G4JAN8MVVD7XWTMY',
              '01EK98JNK5W2R79WH2NPW9DXFJ',
              '01EK98VTJ1BJMGKN1ENMPJ2XZN',
              '01EK994ZGZS6MEHD9GM5JEEQFA',
              '01EK99E4G0MFEZH5VGQTYKYFXX',
              '01EK99Q9F3YDN93NAX8DV17Z20',
              '01EK9A0EDZAWBZQ6BRJS7MSXWX',
              '01EK9A9KCY483V1BNVD0C8T04M',
              '01EK9AJRC1FV2ZEZA0PCJSNN1G',
              '01EKB2XWNHJZGR93H73JEXYHVC'
            ],
            parents: [
              {
                ulid: '01EKB3VZKYW1ASDYE0FB2WNRQR',
                minTime: 1601254800000,
                maxTime: 1601258400000
              },
              {
                ulid: '01EKB626NQ3RSAXGDE1CP8713G',
                minTime: 1601258400000,
                maxTime: 1601262300000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 1478283,
          chunkSize: 29501415
        }
      },
      {
        meta: {
          ulid: '01EK98VTJ33ZG41J57068K957P',
          minTime: 1601259900000,
          maxTime: 1601260200000,
          stats: {
            numSamples: 1379932,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK98VTJ33ZG41J57068K957P'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155983,
          chunkSize: 1291147
        }
      },
      {
        meta: {
          ulid: '01EK97Q6P1ZZE791VSW9YMRQN3',
          minTime: 1601258700000,
          maxTime: 1601259000000,
          stats: {
            numSamples: 1378952,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK97Q6P1ZZE791VSW9YMRQN3'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 154071,
          chunkSize: 1256676
        }
      },
      {
        meta: {
          ulid: '01EKB5GQY2RE5CQE1DX1XFSRWJ',
          minTime: 1601323500000,
          maxTime: 1601323800000,
          stats: {
            numSamples: 6475544,
            numSeries: 2187,
            numChunks: 53551
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB5GQY2RE5CQE1DX1XFSRWJ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 464326,
          chunkSize: 10779956
        }
      },
      {
        meta: {
          ulid: '01EKB47P4QH20KSFF7SZFBA46Z',
          minTime: 1601321700000,
          maxTime: 1601322000000,
          stats: {
            numSamples: 325733,
            numSeries: 2151,
            numChunks: 2151
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB47P4QH20KSFF7SZFBA46Z'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 196932,
          chunkSize: 745700
        }
      },
      {
        meta: {
          ulid: '01EKB47P5S7KGE0E1NE97M5058',
          minTime: 1601321700000,
          maxTime: 1601322000000,
          stats: {
            numSamples: 330417,
            numSeries: 2151,
            numChunks: 2151
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB47P5S7KGE0E1NE97M5058'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 196964,
          chunkSize: 764875
        }
      },
      {
        meta: {
          ulid: '01EKB6320YDNE8KV48ZW9FXZNH',
          minTime: 1601324100000,
          maxTime: 1601324400000,
          stats: {
            numSamples: 6512592,
            numSeries: 2187,
            numChunks: 53235
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB6320YDNE8KV48ZW9FXZNH'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 463398,
          chunkSize: 11516943
        }
      },
      {
        meta: {
          ulid: '01EKB4C41S6C7CK2TBTW72TPDX',
          minTime: 1601322300000,
          maxTime: 1601322600000,
          stats: {
            numSamples: 6326635,
            numSeries: 2187,
            numChunks: 50729
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4C41S6C7CK2TBTW72TPDX'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 455213,
          chunkSize: 14796730
        }
      },
      {
        meta: {
          ulid: '01EKB3VZKYW1ASDYE0FB2WNRQR',
          minTime: 1601254800000,
          maxTime: 1601258400000,
          stats: {
            numSamples: 16533990,
            numSeries: 948,
            numChunks: 142583
          },
          compaction: {
            level: 2,
            sources: [
              '01EK94062YPNMPTY7EXWXEHSKN',
              '01EK949B1Y77YMEJY4VHX2X58R',
              '01EK94JG1076SSH65BHRBM3GAJ',
              '01EK94VMZT4YRJ6QT4DCMX7E7W',
              '01EK954SZ0ME5W5R2FDM1SKB31',
              '01EK95DYXZQVMWXED5W8JKGCEB',
              '01EK95Q3WVZ6GQZAZ3EBMMFP6G',
              '01EK9608VYTEPY5W1WYJ2Z7X89',
              '01EK969DV0Q0BGZBMZJ10SYMXS',
              '01EK96JJT07BC6A2FCPSZ8QBXC',
              '01EK96VQRZQKW0RMND515HTC9Z',
              '01EK974WR0H82P52D7Z13FKW19'
            ],
            parents: [
              {
                ulid: '01EK94062YPNMPTY7EXWXEHSKN',
                minTime: 1601254800000,
                maxTime: 1601255100000
              },
              {
                ulid: '01EK949B1Y77YMEJY4VHX2X58R',
                minTime: 1601255100000,
                maxTime: 1601255400000
              },
              {
                ulid: '01EK94JG1076SSH65BHRBM3GAJ',
                minTime: 1601255400000,
                maxTime: 1601255700000
              },
              {
                ulid: '01EK94VMZT4YRJ6QT4DCMX7E7W',
                minTime: 1601255700000,
                maxTime: 1601256000000
              },
              {
                ulid: '01EK954SZ0ME5W5R2FDM1SKB31',
                minTime: 1601256000000,
                maxTime: 1601256300000
              },
              {
                ulid: '01EK95DYXZQVMWXED5W8JKGCEB',
                minTime: 1601256300000,
                maxTime: 1601256600000
              },
              {
                ulid: '01EK95Q3WVZ6GQZAZ3EBMMFP6G',
                minTime: 1601256600000,
                maxTime: 1601256900000
              },
              {
                ulid: '01EK9608VYTEPY5W1WYJ2Z7X89',
                minTime: 1601256900000,
                maxTime: 1601257200000
              },
              {
                ulid: '01EK969DV0Q0BGZBMZJ10SYMXS',
                minTime: 1601257200000,
                maxTime: 1601257500000
              },
              {
                ulid: '01EK96JJT07BC6A2FCPSZ8QBXC',
                minTime: 1601257500000,
                maxTime: 1601257800000
              },
              {
                ulid: '01EK96VQRZQKW0RMND515HTC9Z',
                minTime: 1601257800000,
                maxTime: 1601258100000
              },
              {
                ulid: '01EK974WR0H82P52D7Z13FKW19',
                minTime: 1601258100000,
                maxTime: 1601258400000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 755963,
          chunkSize: 14019216
        }
      },
      {
        meta: {
          ulid: '01EK994ZH8BHDFQQYZYB5YMHM2',
          minTime: 1601260200000,
          maxTime: 1601260500000,
          stats: {
            numSamples: 1378952,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK994ZH8BHDFQQYZYB5YMHM2'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155879,
          chunkSize: 1316072
        }
      },
      {
        meta: {
          ulid: '01EKB6C6V4JKTR6VRDED6BBC02',
          minTime: 1601324400000,
          maxTime: 1601324700000,
          stats: {
            numSamples: 6468576,
            numSeries: 2187,
            numChunks: 52498
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB6C6V4JKTR6VRDED6BBC02'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 459894,
          chunkSize: 11299548
        }
      },
      {
        meta: {
          ulid: '01EKB475DSQ2CDP8TVNX0CEZGJ',
          minTime: 1601321100000,
          maxTime: 1601321400000,
          stats: {
            numSamples: 2180613,
            numSeries: 2152,
            numChunks: 13842
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB475DSQ2CDP8TVNX0CEZGJ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 255913,
          chunkSize: 4763467
        }
      },
      {
        meta: {
          ulid: '01EKB4N91MV2MQXWRYD89RNQQ5',
          minTime: 1601322600000,
          maxTime: 1601322900000,
          stats: {
            numSamples: 6394016,
            numSeries: 2187,
            numChunks: 51642
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4N91MV2MQXWRYD89RNQQ5'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 459889,
          chunkSize: 11527232
        }
      },
      {
        meta: {
          ulid: '01EKB626NQ3RSAXGDE1CP8713G',
          minTime: 1601258400000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 17640843,
            numSeries: 948,
            numChunks: 151789
          },
          compaction: {
            level: 2,
            sources: [
              '01EK97E1PY37JYP8096VDM6P4W',
              '01EK97Q6P1ZZE791VSW9YMRQN3',
              '01EK980BN2H98SEQ566EY24GBB',
              '01EK989GM0G4JAN8MVVD7XWTMY',
              '01EK98JNK5W2R79WH2NPW9DXFJ',
              '01EK98VTJ1BJMGKN1ENMPJ2XZN',
              '01EK994ZGZS6MEHD9GM5JEEQFA',
              '01EK99E4G0MFEZH5VGQTYKYFXX',
              '01EK99Q9F3YDN93NAX8DV17Z20',
              '01EK9A0EDZAWBZQ6BRJS7MSXWX',
              '01EK9A9KCY483V1BNVD0C8T04M',
              '01EK9AJRC1FV2ZEZA0PCJSNN1G',
              '01EKB2XWNHJZGR93H73JEXYHVC'
            ],
            parents: [
              {
                ulid: '01EK97E1PY37JYP8096VDM6P4W',
                minTime: 1601258400000,
                maxTime: 1601258700000
              },
              {
                ulid: '01EK97Q6P1ZZE791VSW9YMRQN3',
                minTime: 1601258700000,
                maxTime: 1601259000000
              },
              {
                ulid: '01EK980BN2H98SEQ566EY24GBB',
                minTime: 1601259000000,
                maxTime: 1601259300000
              },
              {
                ulid: '01EK989GM0G4JAN8MVVD7XWTMY',
                minTime: 1601259300000,
                maxTime: 1601259600000
              },
              {
                ulid: '01EK98JNK5W2R79WH2NPW9DXFJ',
                minTime: 1601259600000,
                maxTime: 1601259900000
              },
              {
                ulid: '01EK98VTJ1BJMGKN1ENMPJ2XZN',
                minTime: 1601259900000,
                maxTime: 1601260200000
              },
              {
                ulid: '01EK994ZGZS6MEHD9GM5JEEQFA',
                minTime: 1601260200000,
                maxTime: 1601260500000
              },
              {
                ulid: '01EK99E4G0MFEZH5VGQTYKYFXX',
                minTime: 1601260500000,
                maxTime: 1601260800000
              },
              {
                ulid: '01EK99Q9F3YDN93NAX8DV17Z20',
                minTime: 1601260800000,
                maxTime: 1601261100000
              },
              {
                ulid: '01EK9A0EDZAWBZQ6BRJS7MSXWX',
                minTime: 1601261100000,
                maxTime: 1601261400000
              },
              {
                ulid: '01EK9A9KCY483V1BNVD0C8T04M',
                minTime: 1601261400000,
                maxTime: 1601261700000
              },
              {
                ulid: '01EK9AJRC1FV2ZEZA0PCJSNN1G',
                minTime: 1601261700000,
                maxTime: 1601262000000
              },
              {
                ulid: '01EKB2XWNHJZGR93H73JEXYHVC',
                minTime: 1601262000000,
                maxTime: 1601262300000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 818803,
          chunkSize: 15482207
        }
      },
      {
        meta: {
          ulid: '01EK980BN2BQSEARQGMX4V51X4',
          minTime: 1601259000000,
          maxTime: 1601259300000,
          stats: {
            numSamples: 1378954,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK980BN2BQSEARQGMX4V51X4'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155983,
          chunkSize: 1289230
        }
      },
      {
        meta: {
          ulid: '01EKB62QMC8R016AYSEMMA3NP7',
          minTime: 1601254800000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 34190326,
            numSeries: 948,
            numChunks: 294370
          },
          compaction: {
            level: 3,
            sources: [
              '01EK940632E1T8KYDK3W7HFFVT',
              '01EK949B203PWQH41N4VYZTSJ3',
              '01EK94JG14J5P1FK742ARRR87S',
              '01EK94VN00JQB5RGDRP6M8MTVH',
              '01EK954SZ0DSKEQ30M5Z34N4TA',
              '01EK95DYY1AQZJG0TBBZ6010C7',
              '01EK95Q3X2E3MAE2Q2YAB2WADW',
              '01EK9608W335F2F673HEC2P0AM',
              '01EK969DV3K34BEB820MEYYPPE',
              '01EK96JJT1A8NBZ5G958WEJ51K',
              '01EK96VQS1EKGPTBHYZYKV034A',
              '01EK974WR0EHK8YSBPCC8ZES2J',
              '01EK97E1Q161RNZPW8J304XW2X',
              '01EK97Q6P1ZX197JSTZHBQT1RE',
              '01EK980BN2BQSEARQGMX4V51X4',
              '01EK989GM57YXFKGHCV47ARHSQ',
              '01EK98JNK6ZB82VSMRRP3FGM3Z',
              '01EK98VTJ33ZG41J57068K957P',
              '01EK994ZH8BHDFQQYZYB5YMHM2',
              '01EK99E4G14YEYBW82X2TAZH22',
              '01EK99Q9F75D9MKFB3M7J6GTJ3',
              '01EK9A0EE6KN3GT16T5M18GVXP',
              '01EK9A9KE10QPVXF1F8GFNWFZQ',
              '01EK9AJRC5NHM6RFGFGAVYA6VC',
              '01EKB2XWS3K6TNDMBKBA4822WQ'
            ],
            parents: [
              {
                ulid: '01EKB3W81HWWMQPTCB3F0RS7X1',
                minTime: 1601254800000,
                maxTime: 1601258400000
              },
              {
                ulid: '01EKB62F31CC05S9FYJEB1T0D5',
                minTime: 1601258400000,
                maxTime: 1601262300000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 1565091,
          chunkSize: 32479707
        }
      },
      {
        meta: {
          ulid: '01EK9A9KCY483V1BNVD0C8T04M',
          minTime: 1601261400000,
          maxTime: 1601261700000,
          stats: {
            numSamples: 1377953,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9A9KCY483V1BNVD0C8T04M'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 149711,
          chunkSize: 1171385
        }
      },
      {
        meta: {
          ulid: '01EK9A9KE10QPVXF1F8GFNWFZQ',
          minTime: 1601261400000,
          maxTime: 1601261700000,
          stats: {
            numSamples: 1378952,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK9A9KE10QPVXF1F8GFNWFZQ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155895,
          chunkSize: 1296176
        }
      },
      {
        meta: {
          ulid: '01EK1BEN8N5NC7FS1GQPYM56SJ',
          minTime: 1600979298170,
          maxTime: 1600992000000,
          stats: {
            numSamples: 274618944,
            numSeries: 2172,
            numChunks: 2287643
          },
          compaction: {
            level: 3,
            sources: [
              '01EK0X8HBH0REJX0P89MZ5TMFV',
              '01EK0XBMSAKYQ40HHDVRC1JKF7',
              '01EK0XMSS7R26Q1XA1DDX1V7G6',
              '01EK0XXYQ7VRFM2073ZQ4TM8N5',
              '01EK0Y73PAVF2P3F0GXP1MSS3W',
              '01EK0YG8NMHDFTQ66FBREJWBEA',
              '01EK0YSDM6D1FCV5VPY4CE7X7R',
              '01EK0Z2JK6YYYGFMXTG85P58SD',
              '01EK0ZBQJCZD9DABWZMDNAT7SX',
              '01EK0ZMWH4X37NNV1X4SJ97WG8',
              '01EK0ZY1H6T3T2VSRYCB275V83',
              '01EK1076F7Y1FC4JAGW67D93DY',
              '01EK10GBFBNAJRNQ3T09AE5DM6',
              '01EK10SGDAG1T3X11X546GB1DK',
              '01EK112ND8V94RSF4M1S8NTF30',
              '01EK11BTCJY8P3Q5EQ61Y03XBZ',
              '01EK11MZAN35Y0FS85AR4WBESK',
              '01EK11Y496W9JFRZE1KWQQ57GB',
              '01EK127989FVYCKMATTJ5RY789',
              '01EK12GE7A3KYP8V628QG9YDBA',
              '01EK12SK6S9Y06ZAMA7TJS9DV5',
              '01EK132R5KP4XHQAN5TDTHQVAE',
              '01EK13BX4A502HJ693TGGRHMMS',
              '01EK13N23JV01J505FTDHN8XQR',
              '01EK13Y72K3CHF0TKXGP4DS07S',
              '01EK147C1TPRZYYX3J6681FPMT',
              '01EK14GH0P3TCJGG5FXQK50EVH',
              '01EK14SP09EKSWP6M0JK8F7412',
              '01EK152TY61CV71GM9K5HNYH2M',
              '01EK15BZYEXD08WRBMCYEKYTYH',
              '01EK15N4WEJ4HAJFM2TYVMX8CD',
              '01EK15Y9VSAM6AAD9F5MJKBXC6',
              '01EK167ETAADMN3KHZM05HDH51',
              '01EK16GKS7N1PPPAG6QTR68T0P',
              '01EK16SRR5RGF1XT8XVDQKGS6S',
              '01EK172XQA10WVZGNS0MPMMSH3',
              '01EK17C2P4XHTKG4TJM6WM2BXJ',
              '01EK17N7NBF926P80B0ZMEED8P',
              '01EK17YCN3KP8YTFS5KSC926TX',
              '01EK187HKK189A5ERG476QJNNF',
              '01EK18GPJ9HCEYP8XHNMMC8P04',
              '01EK18SVHAPPKFGP1737R5SJ1F',
              '01EK1930G8VZTN6GRSCA4JEFS7'
            ],
            parents: [
              {
                ulid: '01EK14JB7PQ5H8JMPWFWQH26GZ',
                minTime: 1600979298170,
                maxTime: 1600984800000
              },
              {
                ulid: '01EK1B8PDTNAQRKTDCHQR72B7H',
                minTime: 1600984800000,
                maxTime: 1600992000000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 11591847,
          chunkSize: 292458946
        }
      },
      {
        meta: {
          ulid: '01EKB5SWWV1SWJMT08TC8K7J1H',
          minTime: 1601323800000,
          maxTime: 1601324100000,
          stats: {
            numSamples: 6498350,
            numSeries: 2187,
            numChunks: 53596
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB5SWWV1SWJMT08TC8K7J1H'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 466638,
          chunkSize: 12899246
        }
      },
      {
        meta: {
          ulid: '01EK99E4G0MFEZH5VGQTYKYFXX',
          minTime: 1601260500000,
          maxTime: 1601260800000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK99E4G0MFEZH5VGQTYKYFXX'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 109150,
          chunkSize: 1164009
        }
      },
      {
        meta: {
          ulid: '01EK98JNK6ZB82VSMRRP3FGM3Z',
          minTime: 1601259600000,
          maxTime: 1601259900000,
          stats: {
            numSamples: 1378952,
            numSeries: 947,
            numChunks: 11963
          },
          compaction: {
            level: 1,
            sources: [
              '01EK98JNK6ZB82VSMRRP3FGM3Z'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155959,
          chunkSize: 1311723
        }
      },
      {
        meta: {
          ulid: '01EKB2XWNHJZGR93H73JEXYHVC',
          minTime: 1601262000000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 1103198,
            numSeries: 948,
            numChunks: 9694
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB2XWNHJZGR93H73JEXYHVC'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 142771,
          chunkSize: 1075646
        }
      },
      {
        meta: {
          ulid: '01EK6G70MD5QDF3G1B5T2XBNNE',
          minTime: 1600992000000,
          maxTime: 1601164800000,
          stats: {
            numSamples: 183465,
            numSeries: 2512,
            numChunks: 11204
          },
          compaction: {
            level: 4,
            sources: [
              '01EK19C5G0BJC0837JQE99NXG5',
              '01EK19NAE4VE3874SR2BPCC8HH',
              '01EK19YFDY4YA3W0YQF1AV5FW1',
              '01EK1A7MCD5ZTZ8GE205YFP7VS',
              '01EK1AGSB7QRVWADJ0HP650828',
              '01EK1ASYARX516CA5ZGKH7WG06',
              '01EK1B339WW4RZ5BQQ0RA5STQS',
              '01EK1BC8845KC8D9VYQWEGQNKX',
              '01EK1BSA2NB609Q5BKV850ZDTE',
              '01EK1BYJ5ZYCGXGNQV7PX8WAG8',
              '01EK1C7Q545CJRP5NYAB5MGGKR',
              '01EK1CGW4TM3X02XJMYYB3RGH9',
              '01EK1CT133GCZ6WQS81HG7M346',
              '01EK1D361Y722K774JR8PBTXZD',
              '01EK1DCB0ZFRDS4WQ5XFKZY81V',
              '01EK1DNG095XA8JVVAJF7K0PV5',
              '01EK1DYMZ6A3J0WHN61HFGKZ98',
              '01EK1E7SY11JN0PXYDJTQ5VDFC',
              '01EK1EGYX21EY67YHJ3XYTV6NS',
              '01EK1ET3W14BRH9S4YTFG8QFK3',
              '01EK1F38V57T8AK68DZ13H7STZ',
              '01EK1FCDT4A8GQF91088KGA39M',
              '01EK1FNJS5YJD1G2V8NWTCKVZT',
              '01EK1FYQR7YYHS5MNABBX39FZV',
              '01EK1G7WQ9GD5A2PMPQR9GQFFC',
              '01EK1GH1P52CFQDB0VF36K5BHE',
              '01EK1GT6N6NK2RWFQ80FJWD3AF',
              '01EK1H3BM0NSENVSB20WRCNJNF',
              '01EK5VJGG2X2RVQVJK4XQHAEGG',
              '01EK5VVNF0S8RDZQ6RCX4EQF4S',
              '01EK5W4TE15SH8K73HCRY29Z12',
              '01EK5WDZD25T39MKCTSTWRRQ4E',
              '01EK5WQ4C6B6MX8K6NCF2HXMGD',
              '01EK5X09AZKYNYK26F4HB14J12',
              '01EK5X9EA8BPMS9T9NX1MA415F',
              '01EK5XJK8Z9GK1TXSY52TK65E5',
              '01EK5XVR81223WTRPKNX22BEP4',
              '01EK5Y4X6ZX2R3BQHXN9A52N0Z',
              '01EK5YE25YCMM7DFDA93Q2VWVW',
              '01EK5YQ750ZY1W2YSQY5JB9T54',
              '01EK5Z0C3YPYREF9Z9S0DY7MJH',
              '01EK5Z9H2Z2VM3W9JJ8Z6GBN3G',
              '01EK5ZJP284VT4NBNJGAEG2JAH',
              '01EK5ZVV0YNFW0WNX5F195NTRF',
              '01EK605003Y69N1CYJH5G3GY0X',
              '01EK60E4ZQF0PKM0SJPHF3JB3P',
              '01EK60Q9YGZB8TQW2H035CZN5F',
              '01EK610EX2M07HXX24KJ7JAAZG',
              '01EK619KXF339MSX9VNP1AN9ZN',
              '01EK61JRTZC81DT7VGAPEPVQXP',
              '01EK61VXT3AFABZ6G6CN8XXAGY',
              '01EK6252S0ZMEBV982T0669M2J',
              '01EK62E7RHNX05QDWS7WPE2M43',
              '01EK62QCQ4HHE9CY0634HZQGQ2',
              '01EK630HP14CG6M079K9B1B6D8',
              '01EK639PN4QQWCTT86NE72BWJX',
              '01EK63JVKZQDKTT7N8V7J2CSKZ',
              '01EK670JS5AQHWWKC9F01W324N',
              '01EK670S38730HFKF86FMT5E7H',
              '01EK679W834GFGS9W19XFAAA99',
              '01EK67K16BNB7Q3VH9G65CZPZ5',
              '01EK67W655GDYMXAWATR0C66ZV',
              '01EK685B4JYWZVTC09PM0QS1N2',
              '01EK68EG395W5SZ9RRZN14ZJH6',
              '01EK68QN2715433S24KSEKJECC',
              '01EK690T1ZDVKZDD03CYAZY2GF',
              '01EK699Z0BPA06MA1EXDTGYMDV',
              '01EK69K3Z9YA31YD2F3X6B8S6H',
              '01EK69W8Y7P0TZ6XYADG4HFDT8',
              '01EK6A5DXKKRQ9A59CK49HF8EE',
              '01EK6AEJW5277Q986V6DRSPBEP',
              '01EK6AQQVHTDPAY2JBQEWJ0W6F',
              '01EK6B0WTEPNPHZFE75JJXWN5M',
              '01EK6BA1SJE3TRZJK10SGMRTE8',
              '01EK6BK6RFR0BSAYG58FBDAKYF',
              '01EK6BWBQA01A1DW0NT954504P',
              '01EK6C5GQ8GVXNKCR5XB4D3HMJ',
              '01EK6CENNNFFQFDVFC1W075T7K',
              '01EK6CQTMKVTJFMVZF52WX1RDZ',
              '01EK6D0ZKH2MGP1DKM4CKFC6P7',
              '01EK6DA4JMX43S9R4C8TQSA218',
              '01EK6DK9H8WF09MBK28RHKNVAJ',
              '01EK6DWEGCT8C1D73HCRFSHX9M'
            ],
            parents: [
              {
                ulid: '01EK5XNPS3Q0CWS23HKNBN3PVK',
                minTime: 1600992000000,
                maxTime: 1601000400000
              },
              {
                ulid: '01EK62FD28BQZM2ND3AH6Y4PN5',
                minTime: 1601145300000,
                maxTime: 1601150400000
              },
              {
                ulid: '01EK69DMSQHFDSTZAWYPRM02JQ',
                minTime: 1601150400000,
                maxTime: 1601154600000
              },
              {
                ulid: '01EK6FYX5ZZ04YF486XF1DZMXH',
                minTime: 1601157600000,
                maxTime: 1601164800000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 300000
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 312710,
          chunkSize: 2852066
        }
      },
      {
        meta: {
          ulid: '01EK98VTJ1BJMGKN1ENMPJ2XZN',
          minTime: 1601259900000,
          maxTime: 1601260200000,
          stats: {
            numSamples: 1378954,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK98VTJ1BJMGKN1ENMPJ2XZN'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 152911,
          chunkSize: 1194707
        }
      },
      {
        meta: {
          ulid: '01EK6G30JTRQPV5791BJHX34PR',
          minTime: 1600992000000,
          maxTime: 1601164800000,
          stats: {
            numSamples: 526762548,
            numSeries: 2512,
            numChunks: 4381893
          },
          compaction: {
            level: 4,
            sources: [
              '01EK19C5G0BJC0837JQE99NXG5',
              '01EK19NAE4VE3874SR2BPCC8HH',
              '01EK19YFDY4YA3W0YQF1AV5FW1',
              '01EK1A7MCD5ZTZ8GE205YFP7VS',
              '01EK1AGSB7QRVWADJ0HP650828',
              '01EK1ASYARX516CA5ZGKH7WG06',
              '01EK1B339WW4RZ5BQQ0RA5STQS',
              '01EK1BC8845KC8D9VYQWEGQNKX',
              '01EK1BSA2NB609Q5BKV850ZDTE',
              '01EK1BYJ5ZYCGXGNQV7PX8WAG8',
              '01EK1C7Q545CJRP5NYAB5MGGKR',
              '01EK1CGW4TM3X02XJMYYB3RGH9',
              '01EK1CT133GCZ6WQS81HG7M346',
              '01EK1D361Y722K774JR8PBTXZD',
              '01EK1DCB0ZFRDS4WQ5XFKZY81V',
              '01EK1DNG095XA8JVVAJF7K0PV5',
              '01EK1DYMZ6A3J0WHN61HFGKZ98',
              '01EK1E7SY11JN0PXYDJTQ5VDFC',
              '01EK1EGYX21EY67YHJ3XYTV6NS',
              '01EK1ET3W14BRH9S4YTFG8QFK3',
              '01EK1F38V57T8AK68DZ13H7STZ',
              '01EK1FCDT4A8GQF91088KGA39M',
              '01EK1FNJS5YJD1G2V8NWTCKVZT',
              '01EK1FYQR7YYHS5MNABBX39FZV',
              '01EK1G7WQ9GD5A2PMPQR9GQFFC',
              '01EK1GH1P52CFQDB0VF36K5BHE',
              '01EK1GT6N6NK2RWFQ80FJWD3AF',
              '01EK1H3BM0NSENVSB20WRCNJNF',
              '01EK5VJGG2X2RVQVJK4XQHAEGG',
              '01EK5VVNF0S8RDZQ6RCX4EQF4S',
              '01EK5W4TE15SH8K73HCRY29Z12',
              '01EK5WDZD25T39MKCTSTWRRQ4E',
              '01EK5WQ4C6B6MX8K6NCF2HXMGD',
              '01EK5X09AZKYNYK26F4HB14J12',
              '01EK5X9EA8BPMS9T9NX1MA415F',
              '01EK5XJK8Z9GK1TXSY52TK65E5',
              '01EK5XVR81223WTRPKNX22BEP4',
              '01EK5Y4X6ZX2R3BQHXN9A52N0Z',
              '01EK5YE25YCMM7DFDA93Q2VWVW',
              '01EK5YQ750ZY1W2YSQY5JB9T54',
              '01EK5Z0C3YPYREF9Z9S0DY7MJH',
              '01EK5Z9H2Z2VM3W9JJ8Z6GBN3G',
              '01EK5ZJP284VT4NBNJGAEG2JAH',
              '01EK5ZVV0YNFW0WNX5F195NTRF',
              '01EK605003Y69N1CYJH5G3GY0X',
              '01EK60E4ZQF0PKM0SJPHF3JB3P',
              '01EK60Q9YGZB8TQW2H035CZN5F',
              '01EK610EX2M07HXX24KJ7JAAZG',
              '01EK619KXF339MSX9VNP1AN9ZN',
              '01EK61JRTZC81DT7VGAPEPVQXP',
              '01EK61VXT3AFABZ6G6CN8XXAGY',
              '01EK6252S0ZMEBV982T0669M2J',
              '01EK62E7RHNX05QDWS7WPE2M43',
              '01EK62QCQ4HHE9CY0634HZQGQ2',
              '01EK630HP14CG6M079K9B1B6D8',
              '01EK639PN4QQWCTT86NE72BWJX',
              '01EK63JVKZQDKTT7N8V7J2CSKZ',
              '01EK670JS5AQHWWKC9F01W324N',
              '01EK670S38730HFKF86FMT5E7H',
              '01EK679W834GFGS9W19XFAAA99',
              '01EK67K16BNB7Q3VH9G65CZPZ5',
              '01EK67W655GDYMXAWATR0C66ZV',
              '01EK685B4JYWZVTC09PM0QS1N2',
              '01EK68EG395W5SZ9RRZN14ZJH6',
              '01EK68QN2715433S24KSEKJECC',
              '01EK690T1ZDVKZDD03CYAZY2GF',
              '01EK699Z0BPA06MA1EXDTGYMDV',
              '01EK69K3Z9YA31YD2F3X6B8S6H',
              '01EK69W8Y7P0TZ6XYADG4HFDT8',
              '01EK6A5DXKKRQ9A59CK49HF8EE',
              '01EK6AEJW5277Q986V6DRSPBEP',
              '01EK6AQQVHTDPAY2JBQEWJ0W6F',
              '01EK6B0WTEPNPHZFE75JJXWN5M',
              '01EK6BA1SJE3TRZJK10SGMRTE8',
              '01EK6BK6RFR0BSAYG58FBDAKYF',
              '01EK6BWBQA01A1DW0NT954504P',
              '01EK6C5GQ8GVXNKCR5XB4D3HMJ',
              '01EK6CENNNFFQFDVFC1W075T7K',
              '01EK6CQTMKVTJFMVZF52WX1RDZ',
              '01EK6D0ZKH2MGP1DKM4CKFC6P7',
              '01EK6DA4JMX43S9R4C8TQSA218',
              '01EK6DK9H8WF09MBK28RHKNVAJ',
              '01EK6DWEGCT8C1D73HCRFSHX9M'
            ],
            parents: [
              {
                ulid: '01EK5XNPS3Q0CWS23HKNBN3PVK',
                minTime: 1600992000000,
                maxTime: 1601000400000
              },
              {
                ulid: '01EK62FD28BQZM2ND3AH6Y4PN5',
                minTime: 1601145300000,
                maxTime: 1601150400000
              },
              {
                ulid: '01EK69DMSQHFDSTZAWYPRM02JQ',
                minTime: 1601150400000,
                maxTime: 1601154600000
              },
              {
                ulid: '01EK6FYX5ZZ04YF486XF1DZMXH',
                minTime: 1601157600000,
                maxTime: 1601164800000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 21715416,
          chunkSize: 643281222
        }
      },
      {
        meta: {
          ulid: '01EK99Q9F75D9MKFB3M7J6GTJ3',
          minTime: 1601260800000,
          maxTime: 1601261100000,
          stats: {
            numSamples: 1379930,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK99Q9F75D9MKFB3M7J6GTJ3'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155983,
          chunkSize: 1289645
        }
      },
      {
        meta: {
          ulid: '01EKB480MXVYNG1DKHSMN72G42',
          minTime: 1601322000000,
          maxTime: 1601322300000,
          stats: {
            numSamples: 6325081,
            numSeries: 2179,
            numChunks: 50625
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB480MXVYNG1DKHSMN72G42'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 457886,
          chunkSize: 13932058
        }
      },
      {
        meta: {
          ulid: '01EK6G04KD4WQS7AFK3KJ1AKK7',
          minTime: 1600992000000,
          maxTime: 1601164800000,
          stats: {
            numSamples: 526632934,
            numSeries: 2512,
            numChunks: 4383049
          },
          compaction: {
            level: 4,
            sources: [
              '01EK19C5F5VENWMFM7FYRKPF05',
              '01EK19NADQ72YVT7PWYC5E0Z6C',
              '01EK19YFDKC7PXKSEWC4BKZM35',
              '01EK1A7MBY5N1JXF1D0MM6KGWQ',
              '01EK1AGSATBBPCR80XD3KCG3JT',
              '01EK1ASYAKS1WH3QNHQET44595',
              '01EK1B339B02PHCT6M004E9W0V',
              '01EK1BC87TX49GTW6GT5F3105R',
              '01EK1BSB7C22M57AER7D3P8QKN',
              '01EK1BYJ60R79KBDP5XVEMTF37',
              '01EK1C7Q5BA8FRPC1GPCQKF3EA',
              '01EK1CGW4FE3YMQW77AMBFXE86',
              '01EK1CT13547VGBEEFAZVKVPH3',
              '01EK1D362MKN4BARR8JBV98NC4',
              '01EK1DCB1QA35S1EHDQAVGSEEE',
              '01EK1DNG0HB7D04QN72V7YVKS3',
              '01EK1DYMZA152W0DAKN2YXABBG',
              '01EK1E7SY32XX4MFYNW2QVHHRM',
              '01EK1EGYXBZH37EN5HM0JAKASE',
              '01EK1ET3WC022F2JHST33X8H3J',
              '01EK1F38V6Y2KGFZDBGPPSDRCD',
              '01EK1FCDTAKBWWAD93JQN5D35M',
              '01EK1FNJS6T4NZRH0BYRBJRNDJ',
              '01EK1FYQR703F3CARPXXHF6B3R',
              '01EK1G7WQ8QM4GBW2BJCQ89MJ1',
              '01EK1GH1PCCPRF1V5KWK722B6T',
              '01EK1GT6N5ZV7GBVZEGAS4R8JK',
              '01EK1H3BM6W973VRTPVE46DKVM',
              '01EK5VJGG2TFP6DVCC1WRFFGRD',
              '01EK5VVNFHK6SKW9P4Q3151NGJ',
              '01EK5W4TERCAH70TVTE9B41RVR',
              '01EK5WDZD1N0QFYR8JN4SQKJZT',
              '01EK5WQ4C1DSF3J11H3THF0QJS',
              '01EK5X09B2RMTW7PE2X4E4XQT7',
              '01EK5X9EA87QJTPM3J0WNFZS5X',
              '01EK5XJK986S292D4J0QZBSWGF',
              '01EK5XVR8GV6EJEEMNBGH666JH',
              '01EK5Y4X71K5SMG9A8XGWE1SST',
              '01EK5YE26CW66Z3YSPDDQNDZRR',
              '01EK5YQ752XSQTQWXM5CFVFVHT',
              '01EK5Z0C4A3EB1NEK4FGPN6EHX',
              '01EK5Z9H30RWXJFFAHSY2DZRTQ',
              '01EK5ZJP2WKCYYYXPQ6PGKTRGR',
              '01EK5ZVV11R47T9A8NCT5VKDKQ',
              '01EK605005PRHY28N0S7E87WTM',
              '01EK60E50Z0W7YQHAWPCZWRSBM',
              '01EK60Q9Y5Y0RG5QJYBMZVWK7G',
              '01EK610EX0TQAF8KFCN1G4GEBG',
              '01EK619KX1HFY96Q32MSSEXNAP',
              '01EK61JRV1BF17BX79HZ87R7XG',
              '01EK61VXT235H6G1YSAWCXV36N',
              '01EK6252S2AXDNZWNTAYM4KRSM',
              '01EK62E7S8SVPCGEPDYJYMMHYT',
              '01EK62QCQGJADX3NTZT1BR4QMA',
              '01EK630HP8GYQ3ZYNXCKZQ8YWE',
              '01EK639PNM8ZB5HQ0F34R7ZEQM',
              '01EK63JVM1YCTVM9609FHK3CV4',
              '01EK670KC1BG42PW4BK34FFP1W',
              '01EK670S3BE4X6KJACJWAJYNTS',
              '01EK679W8GPJ0X6YEY8BWCZAJN',
              '01EK67K15ZE9WYSGHDVBDF26FD',
              '01EK67W64T6MSYPEH29A2YG3NX',
              '01EK685B465HQZG143BXV6FPGN',
              '01EK68EG2WBWVXTWAMAED9WETH',
              '01EK68QN21XABAQZN743NC1HTC',
              '01EK690T11EE2RMKKPS2657QNC',
              '01EK699YZY7CEHJZHMWYQCVV7E',
              '01EK69K3YTG75ZHVKMGZ7TE4RT',
              '01EK69W8Y07230G1WGEYJ8DS45',
              '01EK6A5DX78ZQXKG0KQ7C8J75C',
              '01EK6AEJVVHPSHP5R9GCER1YGR',
              '01EK6AQQV7GXSFH6E8ETC9GXVK',
              '01EK6B0WT1BVGD7VTD1WMTVAE1',
              '01EK6BA1S8AG3KY124BFY7PN1S',
              '01EK6BK6R4CRFRNRSNF3H7Q308',
              '01EK6BWBPWPR0M7BG5GKKRA1V2',
              '01EK6C5GP4P37D6AFDXDQBB1T9',
              '01EK6CENNCFFK8BPGF3FPMTFKV',
              '01EK6CQTKXX1M5N4NXW9MWR18E',
              '01EK6D0ZK5BANSQGRXAZH87D1E',
              '01EK6DA4J56P5Z7EPTD40FGBPS',
              '01EK6DK9GY9A0NPMEVNB3Z33DM',
              '01EK6DWEFYWE17FTJRZ9V2HS9K'
            ],
            parents: [
              {
                ulid: '01EK5XNEG9MH36E44M0GHQR62T',
                minTime: 1600992000000,
                maxTime: 1601000400000
              },
              {
                ulid: '01EK62F4XTZKM1P2G48S4BJSX7',
                minTime: 1601145300000,
                maxTime: 1601150400000
              },
              {
                ulid: '01EK69D7YKZX3N4S7R3VY52A0D',
                minTime: 1601150400000,
                maxTime: 1601154600000
              },
              {
                ulid: '01EK6FY0RNDXP8GD8QJ3EDG62Y',
                minTime: 1601157600000,
                maxTime: 1601164800000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 21831580,
          chunkSize: 629547672
        }
      },
      {
        meta: {
          ulid: '01EKB62F31CC05S9FYJEB1T0D5',
          minTime: 1601258400000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 17646731,
            numSeries: 948,
            numChunks: 151301
          },
          compaction: {
            level: 2,
            sources: [
              '01EK97E1Q161RNZPW8J304XW2X',
              '01EK97Q6P1ZX197JSTZHBQT1RE',
              '01EK980BN2BQSEARQGMX4V51X4',
              '01EK989GM57YXFKGHCV47ARHSQ',
              '01EK98JNK6ZB82VSMRRP3FGM3Z',
              '01EK98VTJ33ZG41J57068K957P',
              '01EK994ZH8BHDFQQYZYB5YMHM2',
              '01EK99E4G14YEYBW82X2TAZH22',
              '01EK99Q9F75D9MKFB3M7J6GTJ3',
              '01EK9A0EE6KN3GT16T5M18GVXP',
              '01EK9A9KE10QPVXF1F8GFNWFZQ',
              '01EK9AJRC5NHM6RFGFGAVYA6VC',
              '01EKB2XWS3K6TNDMBKBA4822WQ'
            ],
            parents: [
              {
                ulid: '01EK97E1Q161RNZPW8J304XW2X',
                minTime: 1601258400000,
                maxTime: 1601258700000
              },
              {
                ulid: '01EK97Q6P1ZX197JSTZHBQT1RE',
                minTime: 1601258700000,
                maxTime: 1601259000000
              },
              {
                ulid: '01EK980BN2BQSEARQGMX4V51X4',
                minTime: 1601259000000,
                maxTime: 1601259300000
              },
              {
                ulid: '01EK989GM57YXFKGHCV47ARHSQ',
                minTime: 1601259300000,
                maxTime: 1601259600000
              },
              {
                ulid: '01EK98JNK6ZB82VSMRRP3FGM3Z',
                minTime: 1601259600000,
                maxTime: 1601259900000
              },
              {
                ulid: '01EK98VTJ33ZG41J57068K957P',
                minTime: 1601259900000,
                maxTime: 1601260200000
              },
              {
                ulid: '01EK994ZH8BHDFQQYZYB5YMHM2',
                minTime: 1601260200000,
                maxTime: 1601260500000
              },
              {
                ulid: '01EK99E4G14YEYBW82X2TAZH22',
                minTime: 1601260500000,
                maxTime: 1601260800000
              },
              {
                ulid: '01EK99Q9F75D9MKFB3M7J6GTJ3',
                minTime: 1601260800000,
                maxTime: 1601261100000
              },
              {
                ulid: '01EK9A0EE6KN3GT16T5M18GVXP',
                minTime: 1601261100000,
                maxTime: 1601261400000
              },
              {
                ulid: '01EK9A9KE10QPVXF1F8GFNWFZQ',
                minTime: 1601261400000,
                maxTime: 1601261700000
              },
              {
                ulid: '01EK9AJRC5NHM6RFGFGAVYA6VC',
                minTime: 1601261700000,
                maxTime: 1601262000000
              },
              {
                ulid: '01EKB2XWS3K6TNDMBKBA4822WQ',
                minTime: 1601262000000,
                maxTime: 1601262300000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 849355,
          chunkSize: 16632348
        }
      },
      {
        meta: {
          ulid: '01EK1BDVPB75XEZ58YQRYMDY7F',
          minTime: 1600979298069,
          maxTime: 1600992000000,
          stats: {
            numSamples: 274611046,
            numSeries: 2172,
            numChunks: 2287642
          },
          compaction: {
            level: 3,
            sources: [
              '01EK0X8H8JP4P9FKH6MR422XFR',
              '01EK0XBMRZQXY78GM07CDMQH5P',
              '01EK0XMSSE37ADWYT4R37X69WQ',
              '01EK0XXYPR90TMAR86QHH0F50T',
              '01EK0Y73P5FD95CA54AAJN4ZG3',
              '01EK0YG8N6275KFXA4N0QANAXX',
              '01EK0YSDKRXAN0053RVY0CD2H3',
              '01EK0Z2JJVGCDH5PCA16FXD09S',
              '01EK0ZBQHYQBMHSS4DAY6BW8RZ',
              '01EK0ZMWGP8Y9WGQ7ABPJX1ZCW',
              '01EK0ZY1G317XB3XBBBB5ZDPEH',
              '01EK1076EVRKMRJDFTTM6C2GJC',
              '01EK10GBEC7YBC7HEAQ09GPVSW',
              '01EK10SGD0B9HNFQ6JEW20THCE',
              '01EK112ND2D60655YZK701FZGC',
              '01EK11BTB8DXBVBFQM0GWB3P19',
              '01EK11MZ9W36WA1XT29D7CWWCZ',
              '01EK11Y48Q6DJB7JQENRBNRA3W',
              '01EK12797ZA3KHHAPNF89KCZEK',
              '01EK12GE701P4F8WYTNV5KG6FA',
              '01EK12SK6FRC4XJ6XKZCA3Z6ZF',
              '01EK132R53CF5H7VXC615BZN07',
              '01EK13BX4439MK9HRV80K3KSX6',
              '01EK13N237M35ZQQRX3S41Y6F6',
              '01EK13Y720ZWCNADDWXRSYG4XB',
              '01EK147C0WNN5D4CWB1GGHZX51',
              '01EK14GH0A2383JT753FRR1KTS',
              '01EK14SNZFN0DC9K75MG11FBTP',
              '01EK152TXS0ZK0V68NHJWTEF0F',
              '01EK15BZXK3CRN60ZS8JTAPZXJ',
              '01EK15N4VY61DVA3WPW4FQ7CN3',
              '01EK15Y9VK1E695BCY1D8MR96W',
              '01EK167ESW5SW3X7RD9WJVTJ77',
              '01EK16GKS1A9APK8M6YTB9VCC1',
              '01EK16SRQVS17KQ1BPKRY1R2W3',
              '01EK172XPTQD9171V9SBTPGQ3Q',
              '01EK17C2NR8N8CGX79M3J1TWWT',
              '01EK17N7MXRT7AYKR7W93SS78W',
              '01EK17YCKSWSD68G3762B68KX2',
              '01EK187HJWP2FB3AN9TW80XC78',
              '01EK18GPHS6YAB6BH2X31QA01F',
              '01EK18SVH4P7270E5FTG83Y1T2',
              '01EK1930FT6JKK0A4ZMRS77TRT'
            ],
            parents: [
              {
                ulid: '01EK14J2CX0GZ30KPKCT9XPZ5K',
                minTime: 1600979298069,
                maxTime: 1600984800000
              },
              {
                ulid: '01EK1B8BTTVXDJAT7AXXX589FY',
                minTime: 1600984800000,
                maxTime: 1600992000000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 11454715,
          chunkSize: 291925795
        }
      },
      {
        meta: {
          ulid: '01EK97Q6P1ZX197JSTZHBQT1RE',
          minTime: 1601258700000,
          maxTime: 1601259000000,
          stats: {
            numSamples: 1380906,
            numSeries: 948,
            numChunks: 11964
          },
          compaction: {
            level: 1,
            sources: [
              '01EK97Q6P1ZX197JSTZHBQT1RE'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 155935,
          chunkSize: 1351206
        }
      },
      {
        meta: {
          ulid: '01EKB6C6VDRE4PDRHGD5FWV4NX',
          minTime: 1601324400000,
          maxTime: 1601324700000,
          stats: {
            numSamples: 6465541,
            numSeries: 2187,
            numChunks: 53112
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB6C6VDRE4PDRHGD5FWV4NX'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 464426,
          chunkSize: 11194526
        }
      },
      {
        meta: {
          ulid: '01EKB2XWS3K6TNDMBKBA4822WQ',
          minTime: 1601262000000,
          maxTime: 1601262300000,
          stats: {
            numSamples: 1102225,
            numSeries: 947,
            numChunks: 9693
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB2XWS3K6TNDMBKBA4822WQ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 142131,
          chunkSize: 1047198
        }
      },
      {
        meta: {
          ulid: '01EKB4YDZTVV8WFWZCVWZDB26D',
          minTime: 1601322900000,
          maxTime: 1601323200000,
          stats: {
            numSamples: 6525413,
            numSeries: 2187,
            numChunks: 54559
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB4YDZTVV8WFWZCVWZDB26D'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 466901,
          chunkSize: 10661993
        }
      },
      {
        meta: {
          ulid: '01EKB3W81HWWMQPTCB3F0RS7X1',
          minTime: 1601254800000,
          maxTime: 1601258400000,
          stats: {
            numSamples: 16543595,
            numSeries: 948,
            numChunks: 143069
          },
          compaction: {
            level: 2,
            sources: [
              '01EK940632E1T8KYDK3W7HFFVT',
              '01EK949B203PWQH41N4VYZTSJ3',
              '01EK94JG14J5P1FK742ARRR87S',
              '01EK94VN00JQB5RGDRP6M8MTVH',
              '01EK954SZ0DSKEQ30M5Z34N4TA',
              '01EK95DYY1AQZJG0TBBZ6010C7',
              '01EK95Q3X2E3MAE2Q2YAB2WADW',
              '01EK9608W335F2F673HEC2P0AM',
              '01EK969DV3K34BEB820MEYYPPE',
              '01EK96JJT1A8NBZ5G958WEJ51K',
              '01EK96VQS1EKGPTBHYZYKV034A',
              '01EK974WR0EHK8YSBPCC8ZES2J'
            ],
            parents: [
              {
                ulid: '01EK940632E1T8KYDK3W7HFFVT',
                minTime: 1601254800000,
                maxTime: 1601255100000
              },
              {
                ulid: '01EK949B203PWQH41N4VYZTSJ3',
                minTime: 1601255100000,
                maxTime: 1601255400000
              },
              {
                ulid: '01EK94JG14J5P1FK742ARRR87S',
                minTime: 1601255400000,
                maxTime: 1601255700000
              },
              {
                ulid: '01EK94VN00JQB5RGDRP6M8MTVH',
                minTime: 1601255700000,
                maxTime: 1601256000000
              },
              {
                ulid: '01EK954SZ0DSKEQ30M5Z34N4TA',
                minTime: 1601256000000,
                maxTime: 1601256300000
              },
              {
                ulid: '01EK95DYY1AQZJG0TBBZ6010C7',
                minTime: 1601256300000,
                maxTime: 1601256600000
              },
              {
                ulid: '01EK95Q3X2E3MAE2Q2YAB2WADW',
                minTime: 1601256600000,
                maxTime: 1601256900000
              },
              {
                ulid: '01EK9608W335F2F673HEC2P0AM',
                minTime: 1601256900000,
                maxTime: 1601257200000
              },
              {
                ulid: '01EK969DV3K34BEB820MEYYPPE',
                minTime: 1601257200000,
                maxTime: 1601257500000
              },
              {
                ulid: '01EK96JJT1A8NBZ5G958WEJ51K',
                minTime: 1601257500000,
                maxTime: 1601257800000
              },
              {
                ulid: '01EK96VQS1EKGPTBHYZYKV034A',
                minTime: 1601257800000,
                maxTime: 1601258100000
              },
              {
                ulid: '01EK974WR0EHK8YSBPCC8ZES2J',
                minTime: 1601258100000,
                maxTime: 1601258400000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 810879,
          chunkSize: 15847367
        }
      },
      {
        meta: {
          ulid: '01EKB631YT2K7ER8EPEBBM6VHT',
          minTime: 1601324100000,
          maxTime: 1601324400000,
          stats: {
            numSamples: 6510116,
            numSeries: 2187,
            numChunks: 53107
          },
          compaction: {
            level: 1,
            sources: [
              '01EKB631YT2K7ER8EPEBBM6VHT'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 463398,
          chunkSize: 11151260
        }
      },
      {
        meta: {
          ulid: '01EK99E4G14YEYBW82X2TAZH22',
          minTime: 1601260500000,
          maxTime: 1601260800000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK99E4G14YEYBW82X2TAZH22'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 113406,
          chunkSize: 1251930
        }
      },
      {
        meta: {
          ulid: '01EKB3WMWH1KRCSG2Q9DNB3SS3',
          minTime: 1601164800000,
          maxTime: 1601177700000,
          stats: {
            numSamples: 281273183,
            numSeries: 2189,
            numChunks: 2335574
          },
          compaction: {
            level: 3,
            sources: [
              '01EK6E5KETP00A4DP11K32QWY3',
              '01EK6EERDTENYF5AS1VN8BP268',
              '01EK6EQXCYTH2FRQC83KG2HAY2',
              '01EK6F12BVK23W864JW7NCEPTZ',
              '01EK6FA7AWQJQA2NT0RSM0DXNK',
              '01EK6FKCA5XDNGC9SPYW8RFBW6',
              '01EK6FWH8V4NKZV73Z26KRXKV5',
              '01EK6G5PA8S23036DYKB1C63HK',
              '01EK6GEV6VVEH2GKVBB1VD3R31',
              '01EK6GR0640MV0BSWJ4414JEMW',
              '01EK6H154W6T35SZPH9GFVS07C',
              '01EK6HAA3WVFTBY0Z2FSV8SW5V',
              '01EK6HKF2VBPHMD3CG7RT66ZEG',
              '01EK6HWM1W21B7ZWC0YYW9H7H9',
              '01EK6J5S0Y4GZYQ7NB1H439FHZ',
              '01EK6JEXZWPMXVABX8DFCC8M65',
              '01EK6JR2YXETMMZ55Y02EMF58T',
              '01EK6K17Y35AR3S4X4FSG6AWR8',
              '01EK6KACX8QZA09M8BY08D9NV5',
              '01EK6KKHW2TR37MB7SSY3ZCXA4',
              '01EK6KWPV9JRG667GRK4PHPWTA',
              '01EK6M5VT6KPGT6MXYF228SJZ5',
              '01EK6MF0RVEJ6BJS01ZMRNEB13',
              '01EK6MR5RF9V04CXT5DMF2CC72',
              '01EK6N1APX7XEMAS8KWFY5Q3XM',
              '01EK6NAFP8ZJF45BZTV122TKN2',
              '01EK6NKMN1QCQSJM7XKAPV37B8',
              '01EK6NWSKV6WP9M03G4F397J1B',
              '01EK6P5YK0SPHHT0WV99H8KM4T',
              '01EK6PF3J4M1NA6B7QAN8CY85V',
              '01EK6PR8GWFZGS8G7JS74NRCEG',
              '01EK6Q1DFWN5WC2M4AKAQ708WM',
              '01EK6QAJEVNZ2WNT4D2D8581K6',
              '01EK6QKQEP3D3SWBXNN4A8022Q',
              '01EK6QWWD46AZ6R5BYZAY82M8J',
              '01EK6R61BTAK5D1QT7QWYTWZJY',
              '01EK6RF6AXFC77Z6YG3JH5PAKV',
              '01EK6RRBARGQ6YWFEWE5GQ2BZP',
              '01EK6S1G9MQE85V41N7JKT8CS8',
              '01EK6SAN8KV44JKBY0RCK638R8',
              '01EK6SKT75AYCQ7A5B38X6P93Y',
              '01EK6SWZ69AS5M75BJNVRN2QVZ',
              '01EK6T64536MTWNM8NAHFGC6YP'
            ],
            parents: [
              {
                ulid: '01EK6PSMJYDK81WRNSNC4BTTFQ',
                minTime: 1601164800000,
                maxTime: 1601172000000
              },
              {
                ulid: '01EKB3TAY01M0J6SCCFXGNS3YY',
                minTime: 1601172000000,
                maxTime: 1601177700000
              }
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_one'
            },
            downsample: {
              resolution: 0
            },
            source: 'compactor'
          }
        },
        size: {
          indexSize: 11827076,
          chunkSize: 329768796
        }
      },
      {
        meta: {
          ulid: '01EK989GM57YXFKGHCV47ARHSQ',
          minTime: 1601259300000,
          maxTime: 1601259600000,
          stats: {
            numSamples: 1377000,
            numSeries: 459,
            numChunks: 11475
          },
          compaction: {
            level: 1,
            sources: [
              '01EK989GM57YXFKGHCV47ARHSQ'
            ]
          },
          version: 1,
          thanos: {
            labels: {
              monitor: 'prometheus_two'
            },
            downsample: {
              resolution: 0
            },
            source: 'sidecar'
          }
        },
        size: {
          indexSize: 113406,
          chunkSize: 1271667
        }
      }
    ]
    ,
    err: null,
    label: 'monitor',
    refreshedAt: '2020-08-13T10:57:20.820950749Z',
  },
  status: 'success',
};

