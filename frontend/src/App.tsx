import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, GuestOnly, RequireAuth } from "./auth";
import { ChatPage } from "./pages/ChatPage";
import { FriendsPage } from "./pages/FriendsPage";
import { LoginPage } from "./pages/LoginPage";
import { RegisterPage } from "./pages/RegisterPage";
import { SettingsPage } from "./pages/SettingsPage";

export function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route
            path="/login"
            element={<GuestOnly><LoginPage /></GuestOnly>}
          />
          <Route
            path="/register"
            element={<GuestOnly><RegisterPage /></GuestOnly>}
          />
          <Route
            path="/"
            element={<RequireAuth><ChatPage /></RequireAuth>}
          />
          <Route
            path="/chat/:username"
            element={<RequireAuth><ChatPage /></RequireAuth>}
          />
          <Route
            path="/settings"
            element={<RequireAuth><SettingsPage /></RequireAuth>}
          />
          <Route
            path="/friends"
            element={<RequireAuth><FriendsPage /></RequireAuth>}
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}
