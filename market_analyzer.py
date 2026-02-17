import pandas as pd
import requests
from typing import Dict, Optional
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketAnalyzer:
    """
    A class to collect and analyze real-time market data.
    
    This component fetches historical and current market data from various sources,
    processes it into a usable format, and provides insights for strategy generation.
    """

    def __init__(self):
        self.data_sources = ['alpha_vantage', 'yahoo_finance']
        self.market_data = pd.DataFrame()
        
    def fetch_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetches market data from multiple sources and combines it.
        
        Args:
            symbol: The stock ticker symbol to analyze.
            
        Returns:
            DataFrame containing combined historical data if successful,
            None if all sources fail.
        """
        try:
            dfs = []
            for source in self.data_sources:
                df = self._fetch_from_source(symbol, source)
                if df is not None and not df.empty:
                    dfs.append(df)
            
            if not dfs:
                logger.error("No data fetched from any source.")
                return None
                
            combined_data = pd.concat(dfs)
            # Remove duplicates while preserving order
            combined_data = combined_data.groupby(combined_dat.index).first().sort_index()
            self.market_data = combined_data
            
            logger.info(f"Successfully fetched and combined data for {symbol}")
            return combined_data
            
        except Exception as e:
            logger.error(f"Error fetching market data: {str(e)}")
            return None

    def _fetch_from_source(self, symbol: str, source: str) -> Optional[pd.DataFrame]:
        """
        Internal method to fetch data from a single data source.
        
        Args:
            symbol: The stock ticker symbol.
            source: The name of the data source.
            
        Returns:
            DataFrame if successful, None otherwise.
        """
        try:
            if source == 'alpha_vantage':
                response = requests.get(
                    f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&"
                    f"symbol={symbol}&apikey=YOUR_API_KEY"
                )
                data = response.json()
                df = self._parse_alpha_response(data)
                
            elif source == 'yahoo_finance':
                response = requests.get(
                    f"https://query1.finance.yahoo.com/v7/finance/chart/{symbol}"
                )
                data = response.json()
                df = self._parse_yahoo_response(data)
                
            else:
                logger.warning(f"Unknown data source: {source}")
                return None
                
            if not df.empty:
                return df
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {source}: {str(e)}")
            return None
        except KeyError:
            logger.error(f"Invalid data format from {source}")
            return None
            
    def _parse_alpha_response(self, data: Dict) -> pd.DataFrame:
        """
        Parses JSON response from Alpha Vantage.
        
        Args:
            data: The raw JSON response from the API.
            
        Returns:
            DataFrame with parsed market data.
        """
        if 'Time Series (Daily)' not in data:
            return pd.DataFrame()
            
        daily_data = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(daily_data, orient='index')
        df.index = pd.to_datetime(df.index)
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        df = df.astype(float)
        
        return df

    def _parse_yahoo_response(self, data: Dict) -> pd.DataFrame:
        """
        Parses JSON response from Yahoo Finance.
        
        Args:
            data: The raw JSON response from the API.
            
        Returns:
            DataFrame with parsed market data.
        """
        if 'chart' not in data or 'result' not in data['chart']:
            return pd.DataFrame()
            
        result = data['chart']['result'][0]
        timestamp = [pd.Timestamp(x, unit='s') for x in result['timestamp']]
        df = pd.DataFrame({
            'open': result['indicators']['quote'][0]['open'],
            'high': result['indicators']['quote'][0]['high'],
            'low': result['indicators']['quote'][0]['low'],
            'close': result['indicators']['quote'][0]['close'],
            'volume': result['indicators']['quote'][0]['volume']
        }, index=timestamp)
        
        return df

    def get_trend_analysis(self) -> Dict:
        """
        Performs trend analysis on the fetched market data.
        
        Returns:
            Dictionary with trend analysis metrics.
        """
        if self.market_data.empty:
            logger.warning("Market data is empty. Cannot perform analysis.")
            return {}
            
        try:
            # Calculate moving averages
            ma_20 = self.market_data['close'].rolling(20).mean()
            ma_50 = self.market_data['close'].rolling(50).mean()
            ma_200 = self.market_data['close'].rolling(200).mean()
            
            # Determine trend direction
            current_price = self.market_data.iloc[-1