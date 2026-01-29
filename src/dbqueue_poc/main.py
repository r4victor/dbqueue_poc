from dbqueue_poc.app import create_app, register_routes

app = create_app()
register_routes(app)
