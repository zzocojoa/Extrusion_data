import os
from PIL import Image, ImageDraw, ImageFont

def create_logo():
    # Settings
    size = (512, 512)
    bg_color = (0, 0, 0, 0) # Transparent
    # If transparent doesn't work well with some viewers, we can use dark: (26, 26, 26, 255)
    
    # Create image
    img = Image.new('RGBA', size, bg_color)
    draw = ImageDraw.Draw(img)
    
    # Colors
    blue = (59, 142, 208, 255) # #3B8ED0
    green = (44, 201, 133, 255) # #2CC985
    white = (255, 255, 255, 255)
    
    # Draw "Extrusion" shape (Abstract Hexagon / Cube)
    # Center
    cx, cy = size[0] // 2, size[1] // 2
    r = 180
    
    # Outer Hexagon (Blue)
    # Points: (cos(a)*r, sin(a)*r) for a in 0, 60, ...
    import math
    points = []
    for i in range(6):
        angle_deg = 60 * i - 30
        angle_rad = math.radians(angle_deg)
        x = cx + r * math.cos(angle_rad)
        y = cy + r * math.sin(angle_rad)
        points.append((x, y))
    
    draw.polygon(points, outline=blue, width=20)
    
    # Inner "E" or Data lines (Green)
    # Draw 3 horizontal bars
    bar_w = 160
    bar_h = 30
    gap = 50
    
    # Top
    draw.rounded_rectangle(
        (cx - bar_w//2, cy - gap - bar_h, cx + bar_w//2, cy - gap),
        radius=10, fill=green
    )
    # Middle
    draw.rounded_rectangle(
        (cx - bar_w//2, cy, cx + bar_w//4, cy + bar_h),
        radius=10, fill=white
    )
    # Bottom
    draw.rounded_rectangle(
        (cx - bar_w//2, cy + gap + bar_h, cx + bar_w//2, cy + gap + 2*bar_h),
        radius=10, fill=green
    )

    # Save
    base_dir = os.path.dirname(os.path.abspath(__file__))
    assets_dir = os.path.join(os.path.dirname(base_dir), 'assets')
    os.makedirs(assets_dir, exist_ok=True)
    
    png_path = os.path.join(assets_dir, 'logo.png')
    ico_path = os.path.join(assets_dir, 'app.ico')
    
    img.save(png_path)
    print(f"Saved PNG: {png_path}")
    
    # Save as ICO (sizes: 16, 32, 48, 64, 128, 256)
    img.save(ico_path, format='ICO', sizes=[(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)])
    print(f"Saved ICO: {ico_path}")

if __name__ == "__main__":
    create_logo()
