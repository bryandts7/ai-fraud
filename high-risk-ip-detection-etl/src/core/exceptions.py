
# src/core/exceptions.py
class PipelineError(Exception):
    """Base exception for pipeline errors"""
    pass

class ConfigurationError(PipelineError):
    """Configuration related errors"""
    pass

class ExtractionError(PipelineError):
    """Data extraction errors"""
    pass

class TransformationError(PipelineError):
    """Data transformation errors"""
    pass

class LoadError(PipelineError):
    """Data loading errors"""
    pass