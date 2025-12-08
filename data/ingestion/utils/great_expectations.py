"""
Great Expectations validation for data quality checks.
"""

import numpy as np
from typing import List, Dict, Any
from datetime import datetime

class DataQualityValidator:
    """Data quality validation using Great Expectations principles."""
    
    @staticmethod
    def validate_schema_silver(posts: List[Dict]) -> Dict[str, Any]:
        """Validate silver layer schema compliance."""
        results = {
            'passed': True,
            'errors': [],
            'warnings': []
        }
        
        required_fields = ['post_id', 'post_text_cleaned', 'engagement_score', 
                          'post_embedding', 'source_type']
        
        for i, post in enumerate(posts):
            # Check required fields
            for field in required_fields:
                if field not in post:
                    results['passed'] = False
                    results['errors'].append(f"Post {i}: Missing required field '{field}'")
            
            # Check data types
            if 'post_id' in post and not isinstance(post['post_id'], str):
                results['passed'] = False
                results['errors'].append(f"Post {i}: post_id must be string")
            
            if 'engagement_score' in post and not isinstance(post['engagement_score'], (int, float)):
                results['passed'] = False
                results['errors'].append(f"Post {i}: engagement_score must be numeric")
            
            if 'post_embedding' in post:
                embedding = post['post_embedding']
                if not isinstance(embedding, list) or len(embedding) != 384:
                    results['passed'] = False
                    results['errors'].append(f"Post {i}: post_embedding must be array of 384 floats")
            
            if 'source_type' in post and post['source_type'] not in ['Customer', 'Competitor', 'Reviewer']:
                results['passed'] = False
                results['errors'].append(f"Post {i}: source_type must be one of ['Customer', 'Competitor', 'Reviewer']")
        
        return results
    
    @staticmethod
    def validate_data_quality(posts: List[Dict]) -> Dict[str, Any]:
        """Validate data quality metrics."""
        results = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
        
        # Check for null values
        null_text_count = sum(1 for post in posts if not post.get('post_text_cleaned'))
        if null_text_count / len(posts) > 0.01:  # More than 1% null
            results['passed'] = False
            results['errors'].append(f"Too many null post_text_cleaned: {null_text_count}/{len(posts)}")
        
        # Check embedding dimensions
        invalid_embeddings = sum(1 for post in posts 
                                if not isinstance(post.get('post_embedding'), list) 
                                or len(post.get('post_embedding', [])) != 384)
        if invalid_embeddings > 0:
            results['passed'] = False
            results['errors'].append(f"Invalid embedding dimensions: {invalid_embeddings} posts")
        
        # Check engagement score ranges
        engagement_scores = [post.get('engagement_score', 0) for post in posts]
        if engagement_scores:
            max_engagement = max(engagement_scores)
            if max_engagement > 10000000:  # Business rule: no post should have >10M engagement
                results['warnings'].append(f"Suspiciously high engagement score: {max_engagement}")
        
        # Calculate metrics
        results['metrics'] = {
            'total_posts': len(posts),
            'avg_engagement': np.mean(engagement_scores) if engagement_scores else 0,
            'null_text_percentage': (null_text_count / len(posts)) * 100,
            'unique_sources': len(set(post.get('source_type') for post in posts))
        }
        
        return results
    
    @staticmethod
    def validate_gold_layer(similarity_results: List[Dict]) -> Dict[str, Any]:
        """Validate gold layer similarity results."""
        results = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
        
        # Check cosine similarity range
        similarity_scores = [result.get('cosine_similarity_score', 0) for result in similarity_results]
        
        for i, score in enumerate(similarity_scores):
            if not isinstance(score, (int, float)) or score < -1.0 or score > 1.0:
                results['passed'] = False
                results['errors'].append(f"Result {i}: cosine_similarity_score must be between -1.0 and 1.0")
        
        # Check referential integrity
        post_ids = [result.get('post_id') for result in similarity_results]
        unique_posts = len(set(post_ids))
        if unique_posts != len(post_ids):
            results['warnings'].append(f"Duplicate post_ids in results: {len(post_ids) - unique_posts}")
        
        # Calculate metrics
        results['metrics'] = {
            'total_results': len(similarity_results),
            'avg_similarity': np.mean(similarity_scores) if similarity_scores else 0,
            'max_similarity': max(similarity_scores) if similarity_scores else 0,
            'min_similarity': min(similarity_scores) if similarity_scores else 0
        }
        
        return results
