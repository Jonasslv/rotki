import { BalanceState } from '@/store/balances/types';

export const defaultState = (): BalanceState => ({
  eth: {},
  ksm: {},
  btc: {
    standalone: {},
    xpubs: []
  },
  avax: {},
  totals: {},
  liabilities: {},
  usdToFiatExchangeRates: {},
  connectedExchanges: [],
  exchangeBalances: {},
  ethAccounts: [],
  ksmAccounts: [],
  btcAccounts: {
    standalone: [],
    xpubs: []
  },
  avaxAccounts: [],
  supportedAssets: [],
  manualBalances: [],
  manualBalanceByLocation: {},
  prices: {},
  loopringBalances: {}
});

export const state: BalanceState = defaultState();
