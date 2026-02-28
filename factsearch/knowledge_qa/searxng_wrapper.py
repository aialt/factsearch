import asyncio
import aiohttp
import os
import logging
from collections import defaultdict


class SearXNGAPIWrapper:

    def __init__(self, snippet_cnt=10, searxng_url=None):
        self.k = snippet_cnt
        self.gl = "us" 
        self.hl = "en"
        print("SearXNG called")
        
        # get SearXNG url 
        self.searxng_url = searxng_url or os.environ.get("SEARXNG_URL", "http://localhost:8888")
        self.searxng_url = self.searxng_url.rstrip('/')
        
        # check connection to SearXNG
        self._test_connection()
    
    def _test_connection(self):
        try:
            import requests
            response = requests.get(f"{self.searxng_url}/config", timeout=5)
            if response.status_code == 200:
                logging.info(f"SearXNG connection successful at {self.searxng_url}")
            else:
                logging.warning(f"SearXNG responded with status {response.status_code}")
        except Exception as e:
            logging.error(f"Failed to connect to SearXNG at {self.searxng_url}: {e}")
            print(f"Warning: Cannot connect to SearXNG at {self.searxng_url}. Make sure it's running.")
    
    async def _searxng_search_results(self, session, search_term: str, gl: str, hl: str) -> dict:
        """
        Perform search using SearXNG API
        """
        params = {
            'q': search_term,
            'format': 'json',
            'lang': hl,
            'categories': 'general',
            'safesearch': 0
        }
        
        try:
            async with session.get(
                f"{self.searxng_url}/search",
                params=params,
                timeout=10
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logging.error(f"SearXNG search failed with status {response.status}")
                    return {'results': []}
        except Exception as e:
            logging.error(f"SearXNG search error for '{search_term}': {e}")
            return {'results': []}

    def _log_engine_breakdown(self, all_raw_results: list):
        """
        Parses engine attribution across all raw SearXNG results and prints
        a breakdown of how many results (snippets) came from each engine.

        SearXNG returns an 'engines' list per result indicating which engines
        returned that result. This method tallies those across all queries.

        Args:
            all_raw_results: list of raw SearXNG response dicts (one per query)
        """
        engine_counts = defaultdict(int)
        total_results = 0

        for response in all_raw_results:
            if not isinstance(response, dict):
                continue
            for result in response.get('results', []):
                engines = result.get('engines', [])
                for engine in engines:
                    engine_counts[engine.strip().lower()] += 1
                total_results += 1

        if total_results == 0:
            print("\n[SearXNG Engine Breakdown] No results retrieved across all queries.")
            return

        print("\n" + "="*45)
        print("  SearXNG Engine Breakdown (across all queries)")
        print("="*45)
        print(f"  {'Engine':<20} {'Results':>8}  {'Share':>7}")
        print("-"*45)
        for engine, count in sorted(engine_counts.items(), key=lambda x: -x[1]):
            share = (count / total_results) * 100
            print(f"  {engine:<20} {count:>8}  {share:>6.1f}%")
        print("-"*45)
        print(f"  {'TOTAL':<20} {total_results:>8}")
        print("="*45 + "\n")

    def _parse_results(self, results):
        """
        Expected output format:
        [{"content": str, "source": str}, ...]
        """
        snippets = []
        
        searxng_results = results.get('results', [])
        
        if not searxng_results:
            return [{"content": "No good Search Result was found", "source": "None"}]
        
        # Process each result
        for result in searxng_results[:self.k]:
            content = result.get('content', result.get('title', ''))
            source = result.get('url', 'None')
            print(source)
            
            if content:
                element = {
                    "content": content.replace('\n', ' ').strip(),
                    "source": source
                }
                snippets.append(element)
        
        # Default message if nothing found
        if len(snippets) == 0:
            return [{"content": "No good Search Result was found", "source": "None"}]
        
        # Limiting to k/2 snippets to match GoogleSerperAPIWrapper which this file replaces
        snippets = snippets[:int(self.k / 2)]
        
        return snippets
    
    async def parallel_searches(self, search_queries, gl, hl):
        """ Executes searches to SearXNG in parallel batches with a pause in between to avoid blocking"""
        async with aiohttp.ClientSession() as session:
            results = []
            pause = 2.0 #between batches of queries - added to avoid rate limiting
            batch_size = 3  

            for i in range(0, len(search_queries), batch_size):
                batch = search_queries[i:i + batch_size]
                tasks = [
                    self._searxng_search_results(session, query, gl, hl)
                    for query in batch
                ]
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
                if i + batch_size < len(search_queries):
                    await asyncio.sleep(pause)  
            return results
    
    async def run(self, queries):
        """
        Main run method
        Args:
            queries: List of query pairs, e.g. [['query1a', 'query1b'], ['query2a', 'query2b']]
        
        Returns:
            List of snippet lists, matching GoogleSerperAPIWrapper format
        """
        # Flatten queries
        flattened_queries = []
        for sublist in queries:
            if sublist is None:
                sublist = ['None', 'None']
            for item in sublist:
                flattened_queries.append(item)
        
        # Perform searches
        raw_results = await self.parallel_searches(flattened_queries, gl=self.gl, hl=self.hl)

        # Log engine breakdown across all queries
        self._log_engine_breakdown(raw_results)

        # Process results
        snippets_list = []
        for i, result in enumerate(raw_results):
            if isinstance(result, Exception):
                print(f"[Warning] Search query '{flattened_queries[i]}' failed with error: {result}")
                snippets_list.append([{"content": "Search failed", "source": "None"}])
            elif isinstance(result, dict):
                snippets_list.append(self._parse_results(result))
            else:
                print(f"[Warning] Unexpected result type: {type(result)} — skipping.")
                snippets_list.append([{"content": "Unexpected result format", "source": "None"}])
        
        # Split results into pairs
        snippets_split = [
            snippets_list[i] + snippets_list[i+1] 
            for i in range(0, len(snippets_list), 2)
        ]
        
        return snippets_split


if __name__ == "__main__":
    async def test_searxng():
        search = SearXNGAPIWrapper(snippet_cnt=10)
        
        # Test with single query pair 
        test_queries = [["What is the capital of the United States?", "US capital city"]]
        results = await search.run(test_queries)
        
        print("Test Results:")
        for i, result_group in enumerate(results):
            print(f"Query group {i}:")
            for j, result in enumerate(result_group):
                print(f"  Result {j}: {result['content'][:100]}...")
                print(f"  Source: {result['source']}")
    
    # Run test
    asyncio.run(test_searxng())