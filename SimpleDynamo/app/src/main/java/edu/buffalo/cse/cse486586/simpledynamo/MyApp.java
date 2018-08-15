package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Application;
import android.content.Context;

public class MyApp extends Application {

    private static Context sContext;
    @Override
    public void onCreate() {
        sContext = getApplicationContext();
        super.onCreate();
    }

    public static Context getContext() {
        return sContext;
    }
}