import sqlalchemy as sa


def dispose_engines(primary: sa.engine.Engine, vocab: sa.engine.Engine) -> None:
    """Dispose both engines, without disposing a colocated one twice."""
    primary.dispose()
    if vocab is not primary:
        vocab.dispose()
