import unittest
from datetime import date

from config import TestingConfig
from app import create_app, db
from app.models import User, Company


class SuperadminIndexTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app(TestingConfig)
        self.app.testing = True
        with self.app.app_context():
            db.create_all()
            u = User(
                username='admin',
                email='admin@example.com',
                role='zentral_admin',
                active=True,
            )
            u.set_password('secret')
            db.session.add(u)
            db.session.commit()
        self.client = self.app.test_client()

    def tearDown(self):
        with self.app.app_context():
            db.session.remove()
            db.drop_all()

    def _login(self):
        return self.client.post(
            '/auth/login',
            data={'login': 'admin@example.com', 'password': 'secret'},
            follow_redirects=False,
        )

    def test_superadmin_index_empty(self):
        self._login()
        resp = self.client.get('/superadmin/')
        self.assertEqual(resp.status_code, 200)

    def test_superadmin_index_with_companies_and_null_date(self):
        with self.app.app_context():
            db.session.add(Company(name='Empresa A', slug='empresa-a', plan='7-dias-gratis', status='active', subscription_ends_at=None))
            db.session.add(Company(name='Empresa B', slug='empresa-b', plan='1-mes-sistema', status='paused', subscription_ends_at=date(2026, 2, 28)))
            db.session.commit()

        self._login()
        resp = self.client.get('/superadmin/')
        self.assertEqual(resp.status_code, 200)
        body = resp.get_data(as_text=True)
        self.assertIn('Empresa A', body)
        self.assertIn('Empresa B', body)

    def test_superadmin_companies_api_sort_nulls_last(self):
        with self.app.app_context():
            db.session.add(Company(name='Empresa A', slug='empresa-a', plan='7-dias-gratis', status='active', subscription_ends_at=None))
            db.session.add(Company(name='Empresa B', slug='empresa-b', plan='1-mes-sistema', status='paused', subscription_ends_at=date(2026, 2, 28)))
            db.session.commit()

        self._login()
        resp = self.client.get('/superadmin/api/companies?sort=subscription_ends_at&dir=asc&page=1&per_page=25')
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json() or {}
        self.assertTrue(data.get('ok'))
        items = data.get('items') or []
        self.assertEqual(len(items), 2)
        # ASC with NULLs last => Empresa B first, Empresa A last
        self.assertEqual(items[0].get('name'), 'Empresa B')
        self.assertEqual(items[1].get('name'), 'Empresa A')


if __name__ == '__main__':
    unittest.main()
