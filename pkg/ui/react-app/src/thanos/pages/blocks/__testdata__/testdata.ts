import { BlockListProps } from '../Blocks';

export const sampleAPIResponse: { status: string; data: BlockListProps } = {
  data: {
    blocks: [
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 10093065,
          numSamples: 1189126896,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'compactor',
        },
        ulid: '01EEB0ZRSQDJW51W11V4R6YP4T',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4JXZGQMZA9TQ3DHMTET'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592146,
          numSamples: 68999235,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'sidecar',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEG1J4JXZGQMZA9TQ3DHMTET',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 43700,
          numSamples: 422231,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'compactor',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEEXQJ71TT74ZTQZN50CEETE',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H0W5YVXVQZVQN994XCV'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12225933,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H0W5YVXVQZVQN994XCV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ4FPMZXKJE0EXE5P0YCWP'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 559588,
          numSamples: 65808996,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'sidecar',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEAQ4FPMZXKJE0EXE5P0YCWP',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCHTPJ1MZ8KH0SEJZ4E'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781889,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'sidecar',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEF8AGCHTPJ1MZ8KH0SEJZ4E',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4M2S3D16T89PB5WDRMV'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592116,
          numSamples: 68987680,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4M2S3D16T89PB5WDRMV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCBE5VH6P8Y0Q86WJ9P'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781718,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCBE5VH6P8Y0Q86WJ9P',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H09C3TEPJHRHFTCFQD4'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12219174,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'sidecar',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEF75H09C3TEPJHRHFTCFQD4',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZC589NPQX1PMCVSRMXQ',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFY0CBZHMDK7DB2HHPQXW',
            },
          ],
          sources: ['01EEAYZZC589NPQX1PMCVSRMXQ', '01EEEXFY0CBZHMDK7DB2HHPQXW'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 866038,
          numSamples: 111384786,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'compactor',
        },
        ulid: '01EEFA6C2DE4PBZHG9Y2BVWH5C',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ57ECHQ2BRR8M8Y0S1XEK'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 557229,
          numSamples: 65811839,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ57ECHQ2BRR8M8Y0S1XEK',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 10118537,
          numSamples: 1189148290,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'compactor',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEB0R6V1EX65QW2B4A2HT0HH',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 43701,
          numSamples: 422180,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_two',
          },
          source: 'compactor',
        },
        ulid: '01EEEXN994W7CQ7RBMXDDKCX7Q',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZD67C09XCQWBAF925T9',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFYBJCC37WDBHC53ZQ8NZ',
            },
          ],
          sources: ['01EEAYZZD67C09XCQWBAF925T9', '01EEEXFYBJCC37WDBHC53ZQ8NZ'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 877710,
          numSamples: 111417287,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_one',
          },
          source: 'compactor',
          files: [
            {
              rel_path: 'chunks/000001',
              size_bytes: 536870882,
            },
            {
              rel_path: 'chunks/000002',
              size_bytes: 143670,
            },
            {
              rel_path: 'index',
              size_bytes: 257574,
            },
            {
              rel_path: 'meta.json',
            },
          ],
        },
        ulid: '01EEFA63X7DYWPW0AAGX47MY09',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 10093065,
          numSamples: 1189126896,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'compactor',
        },
        ulid: '01EEB0ZRSQDJW51W11V4R6YP4T',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4JXZGQMZA9TQ3DHMTET'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592146,
          numSamples: 68999235,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4JXZGQMZA9TQ3DHMTET',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 43700,
          numSamples: 422231,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'compactor',
        },
        ulid: '01EEEXQJ71TT74ZTQZN50CEETE',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H0W5YVXVQZVQN994XCV'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12225933,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H0W5YVXVQZVQN994XCV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ4FPMZXKJE0EXE5P0YCWP'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 559588,
          numSamples: 65808996,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ4FPMZXKJE0EXE5P0YCWP',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCHTPJ1MZ8KH0SEJZ4E'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781889,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCHTPJ1MZ8KH0SEJZ4E',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4M2S3D16T89PB5WDRMV'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592116,
          numSamples: 68987680,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4M2S3D16T89PB5WDRMV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCBE5VH6P8Y0Q86WJ9P'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781718,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCBE5VH6P8Y0Q86WJ9P',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H09C3TEPJHRHFTCFQD4'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12219174,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H09C3TEPJHRHFTCFQD4',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZC589NPQX1PMCVSRMXQ',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFY0CBZHMDK7DB2HHPQXW',
            },
          ],
          sources: ['01EEAYZZC589NPQX1PMCVSRMXQ', '01EEEXFY0CBZHMDK7DB2HHPQXW'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 866038,
          numSamples: 111384786,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'compactor',
        },
        ulid: '01EEFA6C2DE4PBZHG9Y2BVWH5C',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ57ECHQ2BRR8M8Y0S1XEK'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 557229,
          numSamples: 65811839,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ57ECHQ2BRR8M8Y0S1XEK',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 10118537,
          numSamples: 1189148290,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'compactor',
        },
        ulid: '01EEB0R6V1EX65QW2B4A2HT0HH',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 43701,
          numSamples: 422180,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_abc',
          },
          source: 'compactor',
        },
        ulid: '01EEEXN994W7CQ7RBMXDDKCX7Q',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZD67C09XCQWBAF925T9',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFYBJCC37WDBHC53ZQ8NZ',
            },
          ],
          sources: ['01EEAYZZD67C09XCQWBAF925T9', '01EEEXFYBJCC37WDBHC53ZQ8NZ'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 877710,
          numSamples: 111417287,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_xyz',
          },
          source: 'compactor',
        },
        ulid: '01EEFA63X7DYWPW0AAGX47MY09',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 10093065,
          numSamples: 1189126896,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'compactor',
        },
        ulid: '01EEB0ZRSQDJW51W11V4R6YP4T',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4JXZGQMZA9TQ3DHMTET'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592146,
          numSamples: 68999235,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4JXZGQMZA9TQ3DHMTET',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 43700,
          numSamples: 422231,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'compactor',
        },
        ulid: '01EEEXQJ71TT74ZTQZN50CEETE',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H0W5YVXVQZVQN994XCV'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12225933,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H0W5YVXVQZVQN994XCV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ4FPMZXKJE0EXE5P0YCWP'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 559588,
          numSamples: 65808996,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ4FPMZXKJE0EXE5P0YCWP',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCHTPJ1MZ8KH0SEJZ4E'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781889,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCHTPJ1MZ8KH0SEJZ4E',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4M2S3D16T89PB5WDRMV'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592116,
          numSamples: 68987680,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4M2S3D16T89PB5WDRMV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCBE5VH6P8Y0Q86WJ9P'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781718,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCBE5VH6P8Y0Q86WJ9P',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H09C3TEPJHRHFTCFQD4'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12219174,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H09C3TEPJHRHFTCFQD4',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZC589NPQX1PMCVSRMXQ',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFY0CBZHMDK7DB2HHPQXW',
            },
          ],
          sources: ['01EEAYZZC589NPQX1PMCVSRMXQ', '01EEEXFY0CBZHMDK7DB2HHPQXW'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 866038,
          numSamples: 111384786,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'compactor',
        },
        ulid: '01EEFA6C2DE4PBZHG9Y2BVWH5C',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ57ECHQ2BRR8M8Y0S1XEK'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 557229,
          numSamples: 65811839,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ57ECHQ2BRR8M8Y0S1XEK',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 10118537,
          numSamples: 1189148290,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'compactor',
        },
        ulid: '01EEB0R6V1EX65QW2B4A2HT0HH',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 43701,
          numSamples: 422180,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_ghi',
          },
          source: 'compactor',
        },
        ulid: '01EEEXN994W7CQ7RBMXDDKCX7Q',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZD67C09XCQWBAF925T9',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFYBJCC37WDBHC53ZQ8NZ',
            },
          ],
          sources: ['01EEAYZZD67C09XCQWBAF925T9', '01EEEXFYBJCC37WDBHC53ZQ8NZ'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 877710,
          numSamples: 111417287,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_def',
          },
          source: 'compactor',
        },
        ulid: '01EEFA63X7DYWPW0AAGX47MY09',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 10093065,
          numSamples: 1189126896,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'compactor',
        },
        ulid: '01EEB0ZRSQDJW51W11V4R6YP4T',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4JXZGQMZA9TQ3DHMTET'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592146,
          numSamples: 68999235,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4JXZGQMZA9TQ3DHMTET',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 43700,
          numSamples: 422231,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'compactor',
        },
        ulid: '01EEEXQJ71TT74ZTQZN50CEETE',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H0W5YVXVQZVQN994XCV'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12225933,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H0W5YVXVQZVQN994XCV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ4FPMZXKJE0EXE5P0YCWP'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 559588,
          numSamples: 65808996,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ4FPMZXKJE0EXE5P0YCWP',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCHTPJ1MZ8KH0SEJZ4E'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781889,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCHTPJ1MZ8KH0SEJZ4E',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEG1J4M2S3D16T89PB5WDRMV'],
        },
        maxTime: 1596096000000,
        minTime: 1596088800000,
        stats: {
          numChunks: 592116,
          numSamples: 68987680,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'sidecar',
        },
        ulid: '01EEG1J4M2S3D16T89PB5WDRMV',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF8AGCBE5VH6P8Y0Q86WJ9P'],
        },
        maxTime: 1596088800000,
        minTime: 1596081600000,
        stats: {
          numChunks: 478008,
          numSamples: 55781718,
          numSeries: 2213,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'sidecar',
        },
        ulid: '01EEF8AGCBE5VH6P8Y0Q86WJ9P',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEF75H09C3TEPJHRHFTCFQD4'],
        },
        maxTime: 1596081600000,
        minTime: 1596074400000,
        stats: {
          numChunks: 103328,
          numSamples: 12219174,
          numSeries: 2214,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'sidecar',
        },
        ulid: '01EEF75H09C3TEPJHRHFTCFQD4',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZC589NPQX1PMCVSRMXQ',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFY0CBZHMDK7DB2HHPQXW',
            },
          ],
          sources: ['01EEAYZZC589NPQX1PMCVSRMXQ', '01EEEXFY0CBZHMDK7DB2HHPQXW'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 866038,
          numSamples: 111384786,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'compactor',
        },
        ulid: '01EEFA6C2DE4PBZHG9Y2BVWH5C',
        version: 1,
      },
      {
        compaction: {
          level: 1,
          sources: ['01EEAQ57ECHQ2BRR8M8Y0S1XEK'],
        },
        maxTime: 1595700000000,
        minTime: 1595692800000,
        stats: {
          numChunks: 557229,
          numSamples: 65811839,
          numSeries: 2354,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'sidecar',
        },
        ulid: '01EEAQ57ECHQ2BRR8M8Y0S1XEK',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445200,
              ulid: '01EDBMT400HX6XA3V3RN9PQRCD',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BJH5MJ2381MREZG26J1F0',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6G87QWJ19WCEPPX1BX4',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QEKT35RKDP0MXXMHV1FB',
            },
          ],
          sources: [
            '01ED3ZE8F09HRS5YNF28NNN1S1',
            '01ED445F4DY0H12R7QMKZBQJT1',
            '01ED4B16CN6VZ9ZD8E2KXB3QJH',
            '01ED99SV563D2Y9MFZ66XRWBMR',
            '01ED99TDT6AWW0EQH14YGP2A9H',
            '01ED9FTMCM259036JPMRSZVDNQ',
            '01EDBJYADWH6ME77RGAC8KKZ7A',
            '01EDBVBM4P4TYFA5R7B6NPJAB0',
            '01EDCSA5V5F2RNEDFDCRC952FS',
            '01EDCSAWABPPXQ6PTW2TAN86B9',
            '01EDPTQNCGNGHFRCGXAMXA8RV2',
            '01EDPTQR6NQQTT31G8YHYJX0KX',
            '01EDW1T6G87QWJ19WCEPPX1BX4',
            '01EDW56V4P8B7WHKB3EYHCCF9J',
            '01EE3BD5NK4TRMFEJA0AN952VM',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445200,
        stats: {
          numChunks: 10118537,
          numSamples: 1189148290,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'compactor',
        },
        ulid: '01EEB0R6V1EX65QW2B4A2HT0HH',
        version: 1,
      },
      {
        compaction: {
          level: 4,
          parents: [
            {
              maxTime: 1594663200000,
              minTime: 1594629445222,
              ulid: '01EDBMV5FNTZXBZETENC7ZXY99',
            },
            {
              maxTime: 1594944000000,
              minTime: 1594814400000,
              ulid: '01EE3BKGP8WSJAH3M4Y6D7XQVB',
            },
            {
              maxTime: 1595275200000,
              minTime: 1595268000000,
              ulid: '01EDW1T6FWT1PDSE85WAGBF848',
            },
            {
              maxTime: 1595455200000,
              minTime: 1595440800000,
              ulid: '01EEB0QH11ANV2845HJNEP1M8J',
            },
          ],
          sources: [
            '01ED3ZE8FJ1AR9MSZGXKD2CFSB',
            '01ED445F4J873SDNP2GBG5CKK6',
            '01ED4B16CM43SCSAR45M0CV102',
            '01ED99SV5QV7GMGTR80R5F4WE7',
            '01ED99TDVR2JWEHHQ4JNFDPPCB',
            '01ED9FTMCX9BSV9G792W8J6YA2',
            '01EDBJY6JEQGQT9CQXQZ48X6F9',
            '01EDBVBM4Q0F9SXAD2NV6ER06K',
            '01EDCSA64WQ5JR6P5YFGN25WJV',
            '01EDCSAWBGTEAS9670B7AQBK3P',
            '01EDPTQNC293C3THJEYKS3TXGG',
            '01EDPTQRBTCJTCJ2ENQQNS6RQN',
            '01EDW1T6FWT1PDSE85WAGBF848',
            '01EDW56V4N7AYC99JDH6CSGDWA',
            '01EE3BD5K6NJYVS1ND7PGBCFB6',
          ],
        },
        maxTime: 1595455200000,
        minTime: 1594629445222,
        stats: {
          numChunks: 43701,
          numSamples: 422180,
          numSeries: 2492,
        },
        thanos: {
          downsample: {
            resolution: 300000,
          },
          labels: {
            monitor: 'prometheus_mno',
          },
          source: 'compactor',
        },
        ulid: '01EEEXN994W7CQ7RBMXDDKCX7Q',
        version: 1,
      },
      {
        compaction: {
          level: 2,
          parents: [
            {
              maxTime: 1595944800000,
              minTime: 1595937600000,
              ulid: '01EEAYZZD67C09XCQWBAF925T9',
            },
            {
              maxTime: 1595952000000,
              minTime: 1595944800000,
              ulid: '01EEEXFYBJCC37WDBHC53ZQ8NZ',
            },
          ],
          sources: ['01EEAYZZD67C09XCQWBAF925T9', '01EEEXFYBJCC37WDBHC53ZQ8NZ'],
        },
        maxTime: 1595952000000,
        minTime: 1595937600000,
        stats: {
          numChunks: 877710,
          numSamples: 111417287,
          numSeries: 2684,
        },
        thanos: {
          downsample: {
            resolution: 0,
          },
          labels: {
            monitor: 'prometheus_jkl',
          },
          source: 'compactor',
        },
        ulid: '01EEFA63X7DYWPW0AAGX47MY09',
        version: 1,
      },
    ],
    err: null,
    label: 'monitor',
    refreshedAt: '2020-08-13T10:57:20.820950749Z',
  },
  status: 'success',
};

export const sizeBlock = {
  ulid: '01FT8X9MJF5G7PFRNGZBYT8SCS',
  minTime: 1643123700000,
  maxTime: 1643124000000,
  stats: {
    numSamples: 171320,
    numSeries: 2859,
    numChunks: 2859,
  },
  compaction: {
    level: 1,
    sources: ['01FT8X9MJF5G7PFRNGZBYT8SCS'],
  },
  version: 1,
  thanos: {
    labels: {
      prometheus: 'prom-2 random:2',
    },
    downsample: {
      resolution: 0,
    },
    source: 'sidecar',
    segment_files: ['000001', '000002'],
    files: [
      {
        rel_path: 'chunks/000001',
        size_bytes: 536870882,
      },
      {
        rel_path: 'chunks/000002',
        size_bytes: 143670,
      },
      {
        rel_path: 'index',
        size_bytes: 257574,
      },
      {
        rel_path: 'meta.json',
      },
    ],
  },
};
