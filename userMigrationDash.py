import streamlit as st
import pymysql
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import plotly.express as px
import plotly.graph_objects as go


class MigrationStats:
    def __init__(self, db_config):
        """Initialize database connection and cache settings"""
        self.db_config = db_config
        self.cache_file = 'migration_stats_cache.json'
        self.cache_duration = 300  # 5 minutes in seconds

    def get_db_connection(self):
        """Create database connection"""
        return pymysql.connect(
            host=self.db_config['host'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            database=self.db_config['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )


    def should_update_cache(self):
        """Check if cache exists and is valid, otherwise force update."""
        if not os.path.exists(self.cache_file):
            return True

        try:
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            last_update = datetime.fromisoformat(cache.get('last_update', ''))
            return (datetime.now() - last_update).total_seconds() > self.cache_duration
        except (json.JSONDecodeError, ValueError, KeyError):
            # Cache file is corrupted or empty, so we need to update it
            return True

    def get_migration_stats(self):
        """Get migration statistics, using cache when possible"""
        if not self.should_update_cache():
            with open(self.cache_file, 'r') as f:
                return json.load(f)

        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # Get total users count
            cursor.execute("SELECT COUNT(*) as total FROM users")
            total_users = cursor.fetchone()['total']

            # Get migrated users count
            cursor.execute("SELECT COUNT(*) as migrated FROM users WHERE cm_firebase_token IS NOT NULL")
            migrated_users = cursor.fetchone()['migrated']

            # Get hourly migration data for the last 24 hours
            cursor.execute("""
                SELECT 
                    DATE_FORMAT(updated_at, '%Y-%m-%d %H:00:00') as hour,
                    COUNT(*) as count
                FROM users 
                WHERE cm_firebase_token IS NOT NULL 
                AND updated_at >= NOW() - INTERVAL 24 HOUR
                GROUP BY hour
                ORDER BY hour
            """)
            hourly_data = cursor.fetchall()
            # Convert hour field to string for JSON serialization
            hourly_data = [{**item, 'hour': str(item['hour'])} for item in hourly_data]

            # Get daily migration data for the last 7 days
            cursor.execute("""
                SELECT 
                    DATE(updated_at) as date,
                    COUNT(*) as count
                FROM users 
                WHERE cm_firebase_token IS NOT NULL 
                AND updated_at >= NOW() - INTERVAL 7 DAY
                GROUP BY date
                ORDER BY date
            """)
            daily_data = cursor.fetchall()
            # Convert date field to string for JSON serialization
            daily_data = [{**item, 'date': str(item['date'])} for item in daily_data]

            stats = {
                'total_users': total_users,
                'migrated_users': migrated_users,
                'pending_users': total_users - migrated_users,
                'migration_rate': round((migrated_users / total_users * 100), 2) if total_users > 0 else 0,
                'hourly_data': hourly_data,
                'daily_data': daily_data,
                'last_update': datetime.now().isoformat()
            }

            # Save to cache
            with open(self.cache_file, 'w') as f:
                json.dump(stats, f)

            return stats

        finally:
            cursor.close()
            conn.close()


def main():
    st.set_page_config(
        page_title="User Migration Dashboard",
        page_icon="📊",
        layout="wide"
    )



    db_config = {
        'host': st.secrets["DB_HOST"],
        'user': st.secrets["DB_USER"],
        'password': st.secrets["DB_PASSWORD"],
        'database': st.secrets["DB_NAME"]
    }





    stats_manager = MigrationStats(db_config)

    st.title("📱 User Migration Dashboard")
    st.write("Track user migration progress in real-time")

    # Add auto-refresh checkbox
    auto_refresh = st.sidebar.checkbox("Auto-refresh (5 min)", value=True)

    # Main stats
    stats = stats_manager.get_migration_stats()

    # Create three columns for main metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Total Users",
            f"{stats['total_users']:,}",
        )

    with col2:
        st.metric(
            "Migrated Users",
            f"{stats['migrated_users']:,}",
        )

    with col3:
        st.metric(
            "Migration Rate",
            f"{stats['migration_rate']}%",
        )

    # Progress bar
    st.progress(stats['migration_rate'] / 100)

    # Create two columns for graphs
    col1, col2 = st.columns(2)

    with col1:
        # Daily migration trend
        if stats['daily_data']:
            df_daily = pd.DataFrame(stats['daily_data'])
            fig_daily = px.line(
                df_daily,
                x='date',
                y='count',
                title='Daily Migration Trend (Last 7 Days)'
            )
            st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        # Hourly migration trend
        if stats['hourly_data']:
            df_hourly = pd.DataFrame(stats['hourly_data'])
            fig_hourly = px.bar(
                df_hourly,
                x='hour',
                y='count',
                title='Hourly Migration Activity (Last 24 Hours)'
            )
            st.plotly_chart(fig_hourly, use_container_width=True)

    # Display last update time
    st.sidebar.write(f"Last updated: {datetime.fromisoformat(stats['last_update']).strftime('%Y-%m-%d %H:%M:%S')}")

    # Auto-refresh logic
    if auto_refresh:
        time.sleep(600)  # Wait for 5 seconds
        st.experimental_rerun()


if __name__ == "__main__":
    main()